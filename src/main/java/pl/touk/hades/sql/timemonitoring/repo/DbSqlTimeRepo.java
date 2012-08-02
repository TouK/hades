package pl.touk.hades.sql.timemonitoring.repo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Utils;
import pl.touk.hades.exception.LoadMeasuringException;
import pl.touk.hades.sql.DsSafeConnectionGetter;
import pl.touk.hades.sql.SafeSqlExecutor;
import pl.touk.hades.sql.exception.*;
import pl.touk.hades.sql.timemonitoring.ExceptionEnum;
import pl.touk.hades.sql.timemonitoring.SqlTimeCalculator;
import pl.touk.hades.sql.timemonitoring.State;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;

import static pl.touk.hades.Utils.indent;

public class DbSqlTimeRepo implements SqlTimeRepo {

    private static final Logger logger = LoggerFactory.getLogger(DbSqlTimeRepo.class);

    private static final String table = "HADES_RESULTS";
    private static final String stateTable = "HADES_STATES";

    public static enum ResultColumn {
        ds             ("DS"          , Types.VARCHAR),
        sql            ("SQL_STMT"    , Types.VARCHAR),
        connTimeout    ("CONN_TIMEOUT", Types.INTEGER),
        sqlExecTimeout ("EXEC_TIMEOUT", Types.INTEGER),
        timeNanos      ("TIME_NANOS"  , Types.INTEGER),
        instanceId     ("INSTANCE_ID" , Types.VARCHAR),
        logPrefix      ("LOG_PREFIX"  , Types.VARCHAR),
        ts             ("TS"          , Types.TIMESTAMP);

        private final String name;
        private final int type;

        ResultColumn(String name, int type) {
            this.name = name;
            this.type = type;
        }

        public static String list() {
            return list(false);
        }

        public static String questionMarkList() {
            return list(true);
        }

        public void bind(PreparedStatement ps, Object o) throws SQLException {
            ps.setObject(ordinal() + 1, o, type);
        }

        public void bind(PreparedStatement ps, Object o, int index) throws SQLException {
            ps.setObject(index, o, type);
        }

        private static String list(boolean questionMarks) {
            StringBuilder sb = new StringBuilder(questionMarks ? "?" : values()[0].name);
            for (int i = 1; i < values().length; i++) {
                sb.append(", ").append(questionMarks ? "?" : values()[i].name);
            }
            return sb.toString();
        }

        public String toString() {
            return name;
        }
    }

    public static enum StateColumn {
        modifyTimeMillis        ("MODIFIED_MILLIS", Types.INTEGER),
        instanceId              ("INSTANCE_ID"    , Types.VARCHAR),
        quartCluster            ("QUARTZ_CLUSTER" , Types.VARCHAR),
        mainDs                  ("MAIN_DS"        , Types.VARCHAR),
        failoverDs              ("FAILOVER_DS"    , Types.VARCHAR),
        failover                ("FAILOVER_ACTIVE", Types.BIT),
        measuringDurationMillis ("DURATION_MILLIS", Types.INTEGER),
        mainTimeNanos           ("MAIN_NANOS"     , Types.INTEGER),
        failoverTimeNanos       ("FAILOVER_NANOS" , Types.INTEGER);

        private final String name;
        private final int type;

        StateColumn(String name, int type) {
            this.name = name;
            this.type = type;
        }

        public static String list() {
            return list(false);
        }

        public static String questionMarkList() {
            return list(true);
        }

        public void bind(PreparedStatement ps, Object o) throws SQLException {
            ps.setObject(ordinal() + 1, o, type);
        }

        public void bind(PreparedStatement ps, Object o, int index) throws SQLException {
            ps.setObject(index, o, type);
        }

        private static String list(boolean questionMarks) {
            StringBuilder sb = new StringBuilder(questionMarks ? "?" : values()[0].name);
            for (int i = 1; i < values().length; i++) {
                sb.append(", ").append(questionMarks ? "?" : values()[i].name);
            }
            return sb.toString();
        }

        public String toString() {
            return name;
        }
    }

    private static final String columnList = ResultColumn.list();
    private static final String questionMarkList = ResultColumn.questionMarkList();
    private static final String insert = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + questionMarkList + ")";
    private static final String update =
            "UPDATE " +
                    table + " " +
            "SET " +
                    ResultColumn.timeNanos + " = ?, " +
                    ResultColumn.ts + " = ?, " +
                    ResultColumn.instanceId + " = ?, " +
                    ResultColumn.logPrefix + " = ? " +
            "WHERE " +
                    ResultColumn.ds + " = ? " +
                    "AND " + ResultColumn.sql + " = ? " +
                    "AND " + ResultColumn.connTimeout + " = ? " +
                    "AND " + ResultColumn.sqlExecTimeout + " = ?";
    private static final String select =
            "SELECT " +
                    ResultColumn.timeNanos + "," +
                    ResultColumn.instanceId + "," +
                    ResultColumn.logPrefix + " " +
            "FROM " +
                    table + " " +
            "WHERE " +
                    ResultColumn.ts + " > ? " +
                    "AND " + ResultColumn.ds + " = ? " +
                    "AND " + ResultColumn.sql + " = ? " +
                    "AND (" +
                            ResultColumn.timeNanos + " < " + ExceptionEnum.minErroneousValue() + " " +
                            // Borrow "connTimeout" result if it originates from the same instanceId, i.e. from
                            // the same machine and hence from the same underlying (i.e. contained by a Hades) data
                            // source; also, "connTimeout" result can be borrowed only if it comes from Hades with the
                            // same or greater connection timeout:
                            "OR (" + ResultColumn.timeNanos + " = " + ExceptionEnum.connTimeout.value() + " AND " + ResultColumn.instanceId + " = ? AND " + ResultColumn.connTimeout + " >= ?) " +
                            // Borrow "sqlExecTimeout result only if it comes from Hades with the
                            // same or greater sql execution timeout:
                            "OR (" + ResultColumn.timeNanos + " = " + ExceptionEnum.sqlExecTimeout.value() + " AND " + ResultColumn.sqlExecTimeout + " >= ?)" +
                    ") " +
            "ORDER BY " +
                    ResultColumn.ts + " DESC";

    private static final String stateColumnList = StateColumn.list();
    private static final String stateQuestionMarkList = StateColumn.questionMarkList();
    private static final String insertState = "INSERT INTO " + stateTable + " (" + stateColumnList + ") VALUES (" + stateQuestionMarkList + ")";
    private static final String updateState =
            "UPDATE " +
                    stateTable + " " +
            "SET " +
                    StateColumn.modifyTimeMillis + " = ?, " +
                    StateColumn.failover + " = ?, " +
                    StateColumn.measuringDurationMillis + " = ?, " +
                    StateColumn.instanceId + " = ?, " +
                    StateColumn.mainTimeNanos + " = ?, " +
                    StateColumn.failoverTimeNanos + " = ? " +
            "WHERE " +
                    StateColumn.quartCluster + " = ? " +
                    "AND " + StateColumn.mainDs + " = ? " +
                    "AND " + StateColumn.failoverDs + " = ?";
    private static final String selectState =
            "SELECT " +
                    StateColumn.modifyTimeMillis + ", " +
                    StateColumn.failover + ", " +
                    StateColumn.measuringDurationMillis + ", " +
                    StateColumn.instanceId + ", " +
                    StateColumn.mainTimeNanos + ", " +
                    StateColumn.failoverTimeNanos + " " +
            "FROM " +
                    stateTable + " " +
            "WHERE " +
                    StateColumn.modifyTimeMillis + " > ? " +
                    "AND " + StateColumn.quartCluster + " = ? " +
                    "AND " + StateColumn.mainDs + " = ? " +
                    "AND " + StateColumn.failoverDs + " = ?";

    private int borrowExistingMatchingResultIfYoungerThanMillis = 0;

    private DataSource dataSource;
    private int connTimeoutMillis = 500;
    private int sqlExecTimeout = 200;
    private int sqlExecTimeoutForcingPeriodMillis = 300;

    private ExecutorService externalExecutor;

    private SqlTimeCalculator calc;

    public void init(SqlTimeCalculator calc) {
        Utils.assertNotNull(calc, "calc");
        Utils.assertSame(calc.getSqlTimeRepo(), this, "calc.sqlTimeRepo != this");
        Utils.assertNull(this.calc, "calc");

        this.calc = calc;
    }

    public State getHadesClusterState(String logPrefix, long lowerBound, long[] measuringDurationMillis) throws InterruptedException {
        logger.debug(logPrefix + "getHadesClusterState");
        logPrefix = indent(logPrefix);

        Connection c = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            c = new DsSafeConnectionGetter(connTimeoutMillis, "Hades Repo", dataSource, externalExecutor).getConnectionWithTimeout(logPrefix);
            ps = bindStateParameters(Utils.safelyPrepareStatement(logPrefix, c, "Hades Repo", sqlExecTimeout, selectState), lowerBound);
            new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, "Hades Repo", externalExecutor).execute(logPrefix, ps, false, selectState);
            rs = ps.getResultSet();
            if (rs.next()) {
                long modifyTimeMillis = rs.getLong(StateColumn.modifyTimeMillis.name);
                boolean failover = rs.getBoolean(StateColumn.failover.name);
                measuringDurationMillis[0] = rs.getLong(StateColumn.measuringDurationMillis.name);
                String instanceId = rs.getString(StateColumn.instanceId.name);
                long mainTimeNanos = rs.getLong(StateColumn.mainTimeNanos.name);
                long failoverTimeNanos = rs.getLong(StateColumn.failoverTimeNanos.name);
                logger.info(logPrefix + "borrowing from " + instanceId + " result failover=" + failover +
                        " with modifyTimeMillis=" + modifyTimeMillis + " and measuringDurationMillis=" + measuringDurationMillis[0]);
                return new State(instanceId, modifyTimeMillis, failover, mainTimeNanos, failoverTimeNanos);
            } else {
                return null;
            }
        } catch (SQLException e) {
            logger.error(logPrefix + "failed to borrow state: " + selectState, e);
            return null;
        } catch (LoadMeasuringException e) {
            return null;
        } finally {
            close(logPrefix, rs, ps, c);
        }
    }

    public Long findSqlTimeYoungerThan(String logPrefix, String dsName) throws InterruptedException {
        logger.debug(logPrefix + "findSqlTimeYoungerThan");
        logPrefix = indent(logPrefix);

        Connection c = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            c = new DsSafeConnectionGetter(connTimeoutMillis, "Hades Repo", dataSource, externalExecutor).getConnectionWithTimeout(logPrefix);
            ps = bindParameters(Utils.safelyPrepareStatement(logPrefix, c, dsName, sqlExecTimeout, select), dsName);
            new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, dsName, externalExecutor).execute(logPrefix, ps, false, select);
            rs = ps.getResultSet();
            if (rs.next()) {
                long time = rs.getLong(ResultColumn.timeNanos.name);
                String quartzInstanceId = rs.getString(ResultColumn.instanceId.name);
                String logPrefixFromDb = rs.getString(ResultColumn.logPrefix.name);
                logger.info(logPrefix + "borrowing from " + logPrefixFromDb + " on " + quartzInstanceId + " result " + ExceptionEnum.erroneousValuesAsStr(time));
                return time;
            } else {
                logger.debug(logPrefix + "nothing found");
                return null;
            }
        } catch (SQLException e) {
            logger.error(logPrefix + "failed to borrow sql time", e);
            return null;
        } catch (LoadMeasuringException e) {
            return null;
        } finally {
            close(logPrefix, rs, ps, c);
        }
    }

    private PreparedStatement bindParameters(PreparedStatement ps, String dsName) throws SQLException {
        int i = 1;
        ResultColumn.ts.bind(ps, new Timestamp(new Date().getTime() - borrowExistingMatchingResultIfYoungerThanMillis), i++);
        ResultColumn.ds.bind(ps, dsName, i++);
        ResultColumn.sql.bind(ps, calc.getSql(), i++);
        ResultColumn.instanceId.bind(ps, calc.getSqlTimeBasedTrigger().getSchedulerInstanceId(), i++);
        ResultColumn.connTimeout.bind(ps, calc.getConnTimeoutMillis(), i++);
        ResultColumn.sqlExecTimeout.bind(ps, calc.getSqlExecTimeout(), i++);
        return ps;
    }

    private PreparedStatement bindStateParameters(PreparedStatement ps, long lowerBound) throws SQLException {
        int i = 1;
        StateColumn.modifyTimeMillis.bind(ps, lowerBound, i++);
        StateColumn.quartCluster.bind(ps, calc.getSqlTimeBasedTrigger().getQuartzCluster(), i++);
        StateColumn.mainDs.bind(ps, calc.getHades().getDsName(false), i++);
        StateColumn.failoverDs.bind(ps, calc.getHades().getDsName(true), i++);
        return ps;
    }

    public long storeException(String curRunLogPrefix, String dsName, Exception e) throws InterruptedException {
        if (e instanceof LoadMeasuringException) {
            LoadMeasuringException lme = (LoadMeasuringException) e;
            if (lme instanceof ConnException || lme instanceof SqlExecTimeout) {
                return storeSqlTime(curRunLogPrefix, dsName, ExceptionEnum.valueForException(lme));
            } else {
                return ExceptionEnum.valueForException(lme);
            }
        } else {
            return ExceptionEnum.unexpectedException.value();
        }
    }

    public long storeSqlTime(String logPrefix, String dsName, long time) throws InterruptedException {
        logger.debug(logPrefix + "storeSqlTime");
        logPrefix = indent(logPrefix);

        Connection c = null;
        try {
            c = new DsSafeConnectionGetter(connTimeoutMillis, "Hades Repo", dataSource, externalExecutor).getConnectionWithTimeout(logPrefix);
            c.setAutoCommit(true);
        } catch (InterruptedException e) {
            close(logPrefix, null, null, c);
            logger.error(logPrefix + "interrupted while getting connection before updating result", e);
            throw e;
        } catch (Exception e) {
            close(logPrefix, null, null, c);
            logger.error(logPrefix + "failed to get connection", e);
            return time;
        }

        try {
            int updatedCount = updateSqlTime(logPrefix, c, dsName, time);

            if (updatedCount == 0) {
                logger.debug(logPrefix + "trying insert because nothing updated");
                insertSqlTime(logPrefix, c, dsName, time);
            } else if (updatedCount == 1) {
                logger.debug(logPrefix + "updated result");
            }

            return time;
        } finally {
            close(indent(logPrefix), null, null, c);
        }
    }

    private int updateSqlTime(String logPrefix, Connection c, String dsName, long time) throws InterruptedException {
        logger.debug(logPrefix + "updateSqlTime");
        logPrefix = indent(logPrefix);

        PreparedStatement ps = null;
        final String logSuffix = " for dsName=" + dsName;
        try {
            ps = Utils.safelyPrepareStatement(logPrefix, c, "Hades Repo", sqlExecTimeout, update);

            int i = 1;
            ResultColumn.timeNanos.bind(ps, time, i++);
            ResultColumn.ts.bind(ps, new Timestamp(new Date().getTime()), i++);
            ResultColumn.instanceId.bind(ps, calc.getSqlTimeBasedTrigger().getSchedulerInstanceId(), i++);
            ResultColumn.logPrefix.bind(ps, logPrefix, i++);
            ResultColumn.ds.bind(ps, dsName, i++);
            ResultColumn.sql.bind(ps, calc.getSql(), i++);
            ResultColumn.connTimeout.bind(ps, calc.getConnTimeoutMillis(), i++);
            ResultColumn.sqlExecTimeout.bind(ps, calc.getSqlExecTimeout(), i++);

            SafeSqlExecutor safeSqlExecutor = new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, "Hades Repo", externalExecutor);
            safeSqlExecutor.execute(logPrefix, ps, true, update);
            int updatedCount = safeSqlExecutor.getUpdatedCount();
            if (updatedCount >= 0) {
                return updatedCount;
            } else {
                logger.error(logPrefix + "updated " + updatedCount + " records (expected 1)" + logSuffix);
                return -1;
            }
        } catch (InterruptedException e) {
            logger.error(logPrefix + "interrupted while updating result" + logSuffix, e);
            throw e;
        } catch (Exception e) {
            logger.error(logPrefix + "failed to update sql time in db" + logSuffix, e);
            return -1;
        } finally {
            close(logPrefix, null, ps, null);
        }
    }

    private void insertSqlTime(String logPrefix, Connection c, String dsName, long time) {
        logger.debug(logPrefix + "insertSqlTime");
        logPrefix = indent(logPrefix);

        PreparedStatement insertPs = null;
        try {
            insertPs = Utils.safelyPrepareStatement(logPrefix, c, "Hades Repo", sqlExecTimeout, insert);
            ResultColumn.ds.bind(insertPs, dsName);
            ResultColumn.sql.bind(insertPs, calc.getSql());
            ResultColumn.connTimeout.bind(insertPs, calc.getConnTimeoutMillis());
            ResultColumn.sqlExecTimeout.bind(insertPs, calc.getSqlExecTimeout());
            ResultColumn.timeNanos.bind(insertPs, time);
            ResultColumn.instanceId.bind(insertPs, calc.getSqlTimeBasedTrigger().getSchedulerInstanceId());
            ResultColumn.logPrefix.bind(insertPs, logPrefix);
            ResultColumn.ts.bind(insertPs, new Timestamp(new Date().getTime()));

            new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, "Hades Repo", externalExecutor).execute(logPrefix, insertPs, false, insert);
            logger.debug(logPrefix + "inserted result " + time);
        } catch (Exception e) {
            logger.error(logPrefix + "failed to insert result " + time, e);
        } finally {
            close(logPrefix, null, insertPs, null);
        }
    }

    public void saveHadesClusterState(String logPrefix, State state, long runMethodStartMillis) throws InterruptedException {
        logger.debug(logPrefix + "saveHadesClusterState");
        logPrefix = indent(logPrefix);

        Connection c = null;
        try {
            c = new DsSafeConnectionGetter(connTimeoutMillis, "Hades Repo", dataSource, externalExecutor).getConnectionWithTimeout(logPrefix);
            c.setAutoCommit(true);
        } catch (InterruptedException e) {
            close(logPrefix, null, null, c);
            logger.error(logPrefix + "interrupted while getting connection before updating state", e);
            throw e;
        } catch (Exception e) {
            close(logPrefix, null, null, c);
            logger.error(logPrefix + "failed to get connection", e);
            return;
        }

        try {
            int updatedCount = updateHadesClusterState(logPrefix, c, state, runMethodStartMillis);

            if (updatedCount == 0) {
                logger.debug(logPrefix + "trying insert because nothing updated");
                insertHadesClusterState(logPrefix, c, state, runMethodStartMillis);
            } else if (updatedCount == 1) {
                logger.debug(logPrefix + "updated result");
            }
        } finally {
            close(logPrefix, null, null, c);
        }
    }

    private int updateHadesClusterState(String logPrefix, Connection c, State state, long runMethodStartMillis) throws InterruptedException {
        logger.debug(logPrefix + "updateHadesClusterState");
        logPrefix = indent(logPrefix);

        PreparedStatement ps = null;
        try {
            ps = Utils.safelyPrepareStatement(logPrefix, c, "Hades Repo", sqlExecTimeout, updateState);

            int i = 1;
            StateColumn.modifyTimeMillis.bind(ps, state.getModifyTimeMillis(), i++);
            StateColumn.failover.bind(ps, state.getMachineState().isFailoverActive(), i++);
            int measuringDurationNanosIndex = i++;
            StateColumn.instanceId.bind(ps, calc.getSqlTimeBasedTrigger().getSchedulerInstanceId(), i++);
            StateColumn.mainTimeNanos.bind(ps, calc.getSqlTimeBasedTrigger().getLastFailoverQueryTimeMillis(true), i++);
            StateColumn.failoverTimeNanos.bind(ps, calc.getSqlTimeBasedTrigger().getLastFailoverQueryTimeMillis(false), i++);

            StateColumn.quartCluster.bind(ps, calc.getSqlTimeBasedTrigger().getQuartzCluster(), i++);
            StateColumn.mainDs.bind(ps, calc.getHades().getDsName(false), i++);
            StateColumn.failoverDs.bind(ps, calc.getHades().getDsName(true), i++);

            SafeSqlExecutor safeSqlExecutor = new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, "Hades Repo", externalExecutor);
            StateColumn.measuringDurationMillis.bind(ps, System.currentTimeMillis() - runMethodStartMillis, measuringDurationNanosIndex);
            safeSqlExecutor.execute(logPrefix, ps, true, update);
            int updatedCount = safeSqlExecutor.getUpdatedCount();
            if (updatedCount >= 0) {
                return updatedCount;
            } else {
                logger.error(logPrefix + "updated " + updatedCount + " records (expected 1)");
                return -1;
            }
        } catch (InterruptedException e) {
            logger.error(logPrefix + "interrupted while updating result", e);
            throw e;
        } catch (Exception e) {
            logger.error(logPrefix + "failed to update sql time in db", e);
            return -1;
        } finally {
            close(logPrefix, null, ps, null);
        }
    }

    private void insertHadesClusterState(String logPrefix, Connection c, State state, long runMethodStartMillis) {
        logger.debug(logPrefix + "insertHadesClusterState");
        logPrefix = indent(logPrefix);

        PreparedStatement insertPs = null;
        try {
            insertPs = Utils.safelyPrepareStatement(logPrefix, c, "Hades Repo", sqlExecTimeout, insertState);
            StateColumn.failover.bind(insertPs, state.getMachineState().isFailoverActive());
            StateColumn.failoverDs.bind(insertPs, calc.getHades().getDsName(true));
            StateColumn.mainDs.bind(insertPs, calc.getHades().getDsName(false));
            StateColumn.quartCluster.bind(insertPs, calc.getSqlTimeBasedTrigger().getQuartzCluster());
            StateColumn.instanceId.bind(insertPs, calc.getSqlTimeBasedTrigger().getSchedulerInstanceId());
            StateColumn.modifyTimeMillis.bind(insertPs, state.getModifyTimeMillis());
            StateColumn.mainTimeNanos.bind(insertPs, state.getAvg().getLast());
            StateColumn.failoverTimeNanos.bind(insertPs, state.getAvgFailover().getLast());

            SafeSqlExecutor executor = new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, "Hades Repo", externalExecutor);
            StateColumn.measuringDurationMillis.bind(insertPs, System.currentTimeMillis() - runMethodStartMillis);
            executor.execute(logPrefix, insertPs, false, insert);
            logger.debug(logPrefix + "inserted " + state);
        } catch (Exception e) {
            logger.error(logPrefix + "failed to insert " + state, e);
        } finally {
            close(logPrefix, null, insertPs, null);
        }
    }

    private void close(String logPrefix, ResultSet rs, PreparedStatement ps, Connection c) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error(logPrefix + "failed to close result set");
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.error(logPrefix + "failed to close prepared statement");
            }
        }
        if (c != null) {
            try {
                c.close();
            } catch (SQLException e) {
                logger.error(logPrefix + "failed to close connection");
            }
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setConnTimeoutMillis(int connTimeoutMillis) {
        this.connTimeoutMillis = connTimeoutMillis;
    }

    public void setSqlExecTimeout(int sqlExecTimeout) {
        this.sqlExecTimeout = sqlExecTimeout;
    }

    public void setSqlExecTimeoutForcingPeriodMillis(int sqlExecTimeoutForcingPeriodMillis) {
        this.sqlExecTimeoutForcingPeriodMillis = sqlExecTimeoutForcingPeriodMillis;
    }

    public void setBorrowExistingMatchingResultIfYoungerThanMillis(int borrowExistingMatchingResultIfYoungerThanMillis) {
        this.borrowExistingMatchingResultIfYoungerThanMillis = borrowExistingMatchingResultIfYoungerThanMillis;
    }

    public void setExternalExecutor(ExecutorService externalExecutor) {
        this.externalExecutor = externalExecutor;
    }
}
