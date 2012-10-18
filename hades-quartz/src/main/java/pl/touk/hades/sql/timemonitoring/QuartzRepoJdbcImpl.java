package pl.touk.hades.sql.timemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.exception.LoadMeasuringException;
import pl.touk.hades.sql.SafeSqlExecutor;
import pl.touk.hades.sql.exception.PrepareStmtException;
import pl.touk.hades.sql.exception.SqlExecTimeoutSettingException;

import javax.sql.DataSource;
import java.lang.Exception;
import java.lang.InterruptedException;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.StringBuilder;
import java.lang.System;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.ExecutorService;

public class QuartzRepoJdbcImpl implements QuartzRepo {

    private static final Logger logger = LoggerFactory.getLogger(QuartzRepoJdbcImpl.class);

    private static final String stateTable = "HADES_STATES";

    public static enum StateColumn {
        modifyTimeMillis        ("MODIFIED_MILLIS", Types.INTEGER),
        host                    ("HOST"           , Types.VARCHAR),
        quartzInstance          ("QUARTZ_INSTANCE", Types.VARCHAR),
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

    private static final String stateColumnList = StateColumn.list();
    private static final String stateQuestionMarkList = StateColumn.questionMarkList();
    private static final String insertState =
            "INSERT INTO " + stateTable + " (" + stateColumnList + ") VALUES (" + stateQuestionMarkList + ")";

    private static final String updateState =
            "UPDATE " +
                    stateTable + " " +
            "SET " +
                    StateColumn.modifyTimeMillis + " = ?, " +
                    StateColumn.failover + " = ?, " +
                    StateColumn.host + " = ?, " +
                    StateColumn.quartzInstance + " = ?, " +
                    StateColumn.mainTimeNanos + " = ?, " +
                    StateColumn.failoverTimeNanos + " = ?, " +
                    StateColumn.measuringDurationMillis + " = ? " +
            "WHERE " +
                    StateColumn.quartCluster + " = ? " +
                    "AND " + StateColumn.mainDs + " = ? " +
                    "AND " + StateColumn.failoverDs + " = ?";

    private static final String selectState =
            "SELECT " +
                    StateColumn.modifyTimeMillis + ", " +
                    StateColumn.failover + ", " +
                    StateColumn.measuringDurationMillis + ", " +
                    StateColumn.host + ", " +
                    StateColumn.quartzInstance + ", " +
                    StateColumn.mainTimeNanos + ", " +
                    StateColumn.failoverTimeNanos + " " +
            "FROM " +
                    stateTable + " " +
            "WHERE " +
                    StateColumn.modifyTimeMillis + " > ? " +
                    "AND " + StateColumn.quartCluster + " = ? " +
                    "AND " + StateColumn.mainDs + " = ? " +
                    "AND " + StateColumn.failoverDs + " = ?";

    private final RepoJdbcImpl repo;
    private final String quartzCluster;

    public QuartzRepoJdbcImpl(int borrowExistingMatchingResultIfYoungerThanMillis,
                              DataSource dataSource,
                              int connTimeoutMillis,
                              int sqlExecTimeout,
                              int sqlExecTimeoutForcingPeriodMillis,
                              ExecutorService externalExecutor,
                              String quartzCluster,
                              String host,
                              String schedulerInstanceIdHumanReadable)
    throws UnknownHostException {
        repo = new RepoJdbcImpl(
                borrowExistingMatchingResultIfYoungerThanMillis,
                dataSource,
                connTimeoutMillis,
                sqlExecTimeout,
                sqlExecTimeoutForcingPeriodMillis,
                externalExecutor,
                host,
                schedulerInstanceIdHumanReadable);
        this.quartzCluster = quartzCluster;
    }

    public State getHadesClusterState(MonitorRunLogPrefix logPrefix,
                                      Hades hades,
                                      long lowerBound,
                                      long[] measuringDurationMillis)
            throws InterruptedException {
        logger.debug(logPrefix + "getHadesClusterState");
        logPrefix = logPrefix.indent();
        Connection c = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            c = repo.getConnection(logPrefix);
            ps = prepareStateSelect(logPrefix, c, lowerBound, hades.getMainDsName(), hades.getFailoverDsName());
            repo.execute(logPrefix, ps, false, selectState);
            rs = ps.getResultSet();
            if (rs.next()) {
                return extractState(logPrefix, rs, measuringDurationMillis);
            } else {
                return null;
            }
        } catch (SQLException e) {
            logger.error(logPrefix + "failed to borrow state: " + selectState, e);
            return null;
        } catch (LoadMeasuringException e) {
            return null;
        } finally {
            repo.close(logPrefix, rs, ps, c);
        }
    }

    private PreparedStatement prepareStateSelect(MonitorRunLogPrefix logPrefix,
                                                 Connection c,
                                                 long lowerBound,
                                                 String mainDsName,
                                                 String failoverDsName)
            throws SQLException, SqlExecTimeoutSettingException, PrepareStmtException {

        PreparedStatement ps = repo.prepareStmt(logPrefix, c, selectState);

        int i = 1;
        StateColumn.modifyTimeMillis.bind(ps, lowerBound, i++);
        StateColumn.quartCluster.bind(ps, quartzCluster, i++);
        StateColumn.mainDs.bind(ps, mainDsName, i++);
        StateColumn.failoverDs.bind(ps, failoverDsName, i++);

        return ps;
    }

    private State extractState(MonitorRunLogPrefix logPrefix, ResultSet rs, long[] measuringDurationMillis)
            throws SQLException {
        long modifyTimeMillis       = rs.getLong    (StateColumn.modifyTimeMillis.name);
        boolean failover            = rs.getBoolean (StateColumn.failover.name);
        measuringDurationMillis[0]  = rs.getLong    (StateColumn.measuringDurationMillis.name);
        String host                 = rs.getString  (StateColumn.host.name);
        String quartzInstance       = rs.getString  (StateColumn.quartzInstance.name);
        long mainTimeNanos          = rs.getLong    (StateColumn.mainTimeNanos.name);
        long failoverTimeNanos      = rs.getLong    (StateColumn.failoverTimeNanos.name);
        logger.info(logPrefix + "borrowing from " + quartzInstance + " on host " + host +
                " result failover=" + failover + " with modifyTimeMillis=" + modifyTimeMillis +
                " and measuringDurationMillis=" + measuringDurationMillis[0]);
        return new State(host, quartzInstance, modifyTimeMillis, failover, mainTimeNanos, failoverTimeNanos);
    }

    public void saveHadesClusterState(MonitorRunLogPrefix logPrefix,
                                      Hades hades,
                                      State state,
                                      long runMethodStartMillis) throws InterruptedException {
        logger.debug(logPrefix + "saveHadesClusterState");
        logPrefix = logPrefix.indent();

        Connection c = null;
        try {
            c = repo.getConnection(logPrefix);
            c.setAutoCommit(true);
        } catch (InterruptedException e) {
            repo.close(logPrefix, null, null, c);
            logger.error(logPrefix + "interrupted while getting connection before updating state", e);
            throw e;
        } catch (java.lang.Exception e) {
            repo.close(logPrefix, null, null, c);
            logger.error(logPrefix + "failed to get connection", e);
            return;
        }

        try {
            int updatedCount = updateHadesClusterState(logPrefix, hades, c, state, runMethodStartMillis);

            if (updatedCount == 0) {
                logger.debug(logPrefix + "trying insert because nothing updated");
                insertHadesClusterState(logPrefix, c, state, runMethodStartMillis, hades);
            } else if (updatedCount == 1) {
                logger.debug(logPrefix + "updated result");
            }
        } finally {
            repo.close(logPrefix, null, null, c);
        }
    }

    private int updateHadesClusterState(MonitorRunLogPrefix logPrefix,
                                        Hades hades,
                                        Connection c,
                                        State state,
                                        long runMethodStartMillis)
            throws InterruptedException {
        logger.debug(logPrefix + "updateHadesClusterState");
        logPrefix = logPrefix.indent();

        PreparedStatement ps = null;
        try {
            ps = repo.prepareStmt(logPrefix, c, updateState);
            SafeSqlExecutor safeSqlExecutor = repo.createExecutor();
            bindStateUpdateParameters(
                    ps,
                    state,
                    runMethodStartMillis,
                    hades.getMainDsName(),
                    hades.getFailoverDsName());
            safeSqlExecutor.execute(logPrefix, ps, true, updateState);

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
            repo.close(logPrefix, null, ps, null);
        }
    }

    private void bindStateUpdateParameters(PreparedStatement ps,
                                           State state,
                                           long runMethodStartMillis,
                                           String mainDsName,
                                           String failoverDsName)
            throws SQLException {
        int i = 1;

        StateColumn.modifyTimeMillis.bind(ps, state.getModifyTimeMillis(), i++);
        StateColumn.failover.bind(ps, state.getMachineState().isFailoverActive(), i++);
        StateColumn.host.bind(ps, repo.getHost(), i++);
        StateColumn.quartzInstance.bind(ps, getRepoId(), i++);
        StateColumn.mainTimeNanos.bind(ps, state.getAvg().getLast(), i++);
        StateColumn.failoverTimeNanos.bind(ps, state.getAvgFailover().getLast(), i++);
        int measuringDurationNanosIndex = i++;
        StateColumn.quartCluster.bind(ps, quartzCluster, i++);
        StateColumn.mainDs.bind(ps, mainDsName, i++);
        StateColumn.failoverDs.bind(ps, failoverDsName, i++);

        StateColumn.measuringDurationMillis.bind(
                ps,
                System.currentTimeMillis() - runMethodStartMillis,
                measuringDurationNanosIndex);
    }

    private void insertHadesClusterState(MonitorRunLogPrefix logPrefix,
                                         Connection c,
                                         State state,
                                         long runMethodStartMillis,
                                         Hades hades) {
        logger.debug(logPrefix + "insertHadesClusterState");
        logPrefix = logPrefix.indent();

        PreparedStatement ps = null;
        try {
            SafeSqlExecutor executor = repo.createExecutor();
            ps = repo.prepareStmt(logPrefix, c, insertState);
            bindStateInsertParameters(
                    ps,
                    state,
                    runMethodStartMillis,
                    hades.getMainDsName(),
                    hades.getFailoverDsName());
            executor.execute(logPrefix, ps, false, insertState);
            logger.debug(logPrefix + "inserted " + state);
        } catch (Exception e) {
            logger.error(logPrefix + "failed to insert " + state, e);
        } finally {
            repo.close(logPrefix, null, ps, null);
        }
    }

    private void bindStateInsertParameters(PreparedStatement ps,
                                           State state,
                                           long runMethodStartMillis,
                                           String mainDsName,
                                           String failoverDsName)
            throws SQLException {
        StateColumn.failover.bind(ps, state.getMachineState().isFailoverActive());
        StateColumn.failoverDs.bind(ps, failoverDsName);
        StateColumn.mainDs.bind(ps, mainDsName);
        StateColumn.quartCluster.bind(ps, quartzCluster);
        StateColumn.host.bind(ps, repo.getHost());
        StateColumn.quartzInstance.bind(ps, getSchedulerInstanceHumanReadable());
        StateColumn.modifyTimeMillis.bind(ps, state.getModifyTimeMillis());
        StateColumn.mainTimeNanos.bind(ps, state.getAvg().getLast());
        StateColumn.failoverTimeNanos.bind(ps, state.getAvgFailover().getLast());
        StateColumn.measuringDurationMillis.bind(ps, System.currentTimeMillis() - runMethodStartMillis);
    }

    public Long findSqlTimeYoungerThan(MonitorRunLogPrefix curRunLogPrefix, String dsName, String sql)
            throws InterruptedException {
        return repo.findSqlTimeYoungerThan(curRunLogPrefix, dsName, sql);
    }

    public long storeSqlTime(MonitorRunLogPrefix curRunLogPrefix, Hades hades, String dsName, long time, String sql)
            throws InterruptedException {
        return repo.storeSqlTime(curRunLogPrefix, hades, dsName, time, sql);
    }

    public long storeException(MonitorRunLogPrefix curRunLogPrefix, Hades hades, String dsName, Exception e, String sql)
            throws InterruptedException {
        return repo.storeException(curRunLogPrefix, hades, dsName, e, sql);
    }

    public String getHost() {
        return repo.getHost();
    }

    public String getRepoId() {
        return repo.getRepoId();
    }

    public String getSchedulerInstanceHumanReadable() {
        return getRepoId();
    }
}
