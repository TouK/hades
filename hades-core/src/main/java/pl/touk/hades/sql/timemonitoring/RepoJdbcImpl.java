package pl.touk.hades.sql.timemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Utils;
import pl.touk.hades.exception.LoadMeasuringException;
import pl.touk.hades.exception.UnexpectedException;
import pl.touk.hades.sql.DsSafeConnectionGetter;
import pl.touk.hades.sql.SafeSqlExecutor;
import pl.touk.hades.sql.exception.*;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;

import static pl.touk.hades.Utils.indent;

public class RepoJdbcImpl implements Repo {

    private final static Logger logger = LoggerFactory.getLogger(RepoJdbcImpl.class);

    private final static String hadesRepoDesc = "Hades Repo";
    
    private final static String table = "HADES_RESULTS";

    private static enum ResultColumn {
        ds             ("DS"          , Types.VARCHAR),
        sql            ("SQL_STMT"    , Types.VARCHAR),
        connTimeout    ("CONN_TIMEOUT", Types.INTEGER),
        sqlExecTimeout ("EXEC_TIMEOUT", Types.INTEGER),
        timeNanos      ("TIME_NANOS"  , Types.INTEGER),
        host           ("HOST"        , Types.VARCHAR),
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

    private final static String columnList = ResultColumn.list();
    private final static String questionMarkList = ResultColumn.questionMarkList();
    private final static String insert = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + questionMarkList + ")";
    private final static String update =
            "UPDATE " +
                    table + " " +
            "SET " +
                    ResultColumn.timeNanos + " = ?, " +
                    ResultColumn.ts + " = ?, " +
                    ResultColumn.host + " = ?, " +
                    ResultColumn.logPrefix + " = ? " +
            "WHERE " +
                    ResultColumn.ds + " = ? " +
                    "AND " + ResultColumn.sql + " = ? " +
                    "AND " + ResultColumn.connTimeout + " = ? " +
                    "AND " + ResultColumn.sqlExecTimeout + " = ?";
    private final static String select =
            "SELECT " +
                    ResultColumn.timeNanos + "," +
                    ResultColumn.host + "," +
                    ResultColumn.logPrefix + " " +
            "FROM " +
                    table + " " +
            "WHERE " +
                    ResultColumn.ts + " > ? " +
                    // It is assumed that each data source is uniquely identified by its
                    // names across all hadeses using the same repo.
                    "AND " + ResultColumn.ds + " = ? " +
                    "AND " + ResultColumn.sql + " = ? " +
                    "AND (" +
                            ResultColumn.timeNanos + " < " + ExceptionEnum.minErroneousValue() + " " +
                            // Borrow "connTimeout" result if it originates from the same host;
                            // also, "connTimeout" result can be
                            // borrowed only if it comes from Hades with the
                            // same or greater connection timeout:
                            "OR (" + ResultColumn.timeNanos  + " = " + ExceptionEnum.connTimeout.value() + " AND "
                                   + ResultColumn.host + " = ? "
                                   + "AND " + ResultColumn.connTimeout + " >= ?) " +
                            // Borrow "sqlExecTimeout" result only if it comes from Hades with the
                            // same or greater sql execution timeout (and from any host):
                            "OR (" + ResultColumn.timeNanos + " = " + ExceptionEnum.sqlExecTimeout.value() + " AND "
                                   + ResultColumn.sqlExecTimeout + " >= ?)" +
                    ") " +
            "ORDER BY " +
                    ResultColumn.ts + " DESC";

    private final int borrowExistingMatchingResultIfYoungerThanMillis;

    private final DataSource dataSource;
    private final int connTimeoutMillis;
    private final int sqlExecTimeout;
    private final int sqlExecTimeoutForcingPeriodMillis;

    private final ExecutorService externalExecutor;

    private final String host;

    public RepoJdbcImpl(int borrowExistingMatchingResultIfYoungerThanMillis,
                        DataSource dataSource,
                        int connTimeoutMillis,
                        int sqlExecTimeout,
                        int sqlExecTimeoutForcingPeriodMillis,
                        ExecutorService externalExecutor)
            throws UnknownHostException {
        this.borrowExistingMatchingResultIfYoungerThanMillis = borrowExistingMatchingResultIfYoungerThanMillis;
        this.dataSource = dataSource;
        this.connTimeoutMillis = connTimeoutMillis;
        this.sqlExecTimeout = sqlExecTimeout;
        this.sqlExecTimeoutForcingPeriodMillis = sqlExecTimeoutForcingPeriodMillis;
        this.externalExecutor = externalExecutor;
        this.host = InetAddress.getLocalHost().getHostName();
    }

    Connection getConnection(String logPrefix)
            throws UnexpectedException, ConnException, ConnTimeout, InterruptedException {
        return new DsSafeConnectionGetter(
                connTimeoutMillis,
                hadesRepoDesc,
                dataSource,
                externalExecutor
        ).getConnectionWithTimeout(logPrefix);
    }

    SafeSqlExecutor createExecutor() {
        return createExecutor(hadesRepoDesc);
    }

    SafeSqlExecutor createExecutor(String dsName) {
        return new SafeSqlExecutor(
                sqlExecTimeout,
                sqlExecTimeoutForcingPeriodMillis,
                dsName,
                externalExecutor);
    }

    void execute(String logPrefix, PreparedStatement preparedStatement, boolean update, String sql)
            throws SqlExecException, SqlExecTimeout, UnexpectedException, InterruptedException {
        execute(logPrefix, preparedStatement, update, sql, hadesRepoDesc);
    }

    void execute(String logPrefix, PreparedStatement preparedStatement, boolean update, String sql, String dsName)
            throws SqlExecException, SqlExecTimeout, UnexpectedException, InterruptedException {
        createExecutor(dsName).execute(logPrefix, preparedStatement, update, sql);
    }

    PreparedStatement prepareStmt(String logPrefix, Connection c, String sql)
            throws SqlExecTimeoutSettingException, PrepareStmtException {
        return Utils.safelyPrepareStatement(logPrefix, c, hadesRepoDesc, sqlExecTimeout, sql);
    }

    PreparedStatement prepareStmt(String logPrefix, Connection c, String sql, String dsName)
            throws SqlExecTimeoutSettingException, PrepareStmtException {
        return Utils.safelyPrepareStatement(logPrefix, c, dsName, sqlExecTimeout, sql);
    }

    public Long findSqlTimeYoungerThan(String logPrefix, String dsName, String sql) throws InterruptedException {
        logger.debug(logPrefix + "findSqlTimeYoungerThan");
        logPrefix = indent(logPrefix);

        Connection c = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            c = getConnection(logPrefix);
            ps = bindSelectParams(logPrefix, c, dsName, sql);
            execute(logPrefix, ps, false, select, dsName);
            rs = ps.getResultSet();
            if (rs.next()) {
                return borrowTime(logPrefix, rs);
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

    private long borrowTime(String logPrefix, ResultSet rs) throws SQLException {
        long time = rs.getLong(ResultColumn.timeNanos.name);
        String host = rs.getString(ResultColumn.host.name);
        String logPrefixFromDb = rs.getString(ResultColumn.logPrefix.name);
        logger.info(logPrefix + "borrowing from " + logPrefixFromDb + " on "
                + host + " result " + ExceptionEnum.erroneousValuesAsStr(time));
        return time;
    }

    private PreparedStatement bindSelectParams(String logPrefix, Connection c, String dsName, String sql)
            throws SQLException, SqlExecTimeoutSettingException, PrepareStmtException {
        PreparedStatement ps = prepareStmt(logPrefix, c, select, dsName);
        int i = 1;
        ResultColumn.ts.bind(ps, new Timestamp(new Date().getTime() - borrowExistingMatchingResultIfYoungerThanMillis), i++);
        ResultColumn.ds.bind(ps, dsName, i++);
        ResultColumn.sql.bind(ps, sql, i++);
        ResultColumn.host.bind(ps, host, i++);
        ResultColumn.connTimeout.bind(ps, connTimeoutMillis, i++);
        ResultColumn.sqlExecTimeout.bind(ps, sqlExecTimeout, i++);
        return ps;
    }

    public long storeException(String curRunLogPrefix, String dsName, Exception e, String sql)
            throws InterruptedException {
        if (e instanceof LoadMeasuringException) {
            LoadMeasuringException lme = (LoadMeasuringException) e;
            if (lme instanceof ConnException || lme instanceof SqlExecTimeout) {
                return storeSqlTime(curRunLogPrefix, dsName, ExceptionEnum.valueForException(lme), sql);
            } else {
                return ExceptionEnum.valueForException(lme);
            }
        } else {
            return ExceptionEnum.unexpectedException.value();
        }
    }

    public long storeSqlTime(String logPrefix, String dsName, long time, String sql) throws InterruptedException {
        logger.debug(logPrefix + "storeSqlTime");
        logPrefix = indent(logPrefix);

        Connection c = null;
        try {
            c = getConnection(logPrefix);
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
            int updatedCount = updateSqlTime(logPrefix, c, dsName, time, sql);

            if (updatedCount == 0) {
                logger.debug(logPrefix + "trying insert because nothing updated");
                insertSqlTime(logPrefix, c, dsName, time, sql);
            } else if (updatedCount == 1) {
                logger.debug(logPrefix + "updated result");
            }

            return time;
        } finally {
            close(indent(logPrefix), null, null, c);
        }
    }

    private int updateSqlTime(String logPrefix, Connection c, String dsName, long time, String sql) throws InterruptedException {
        logger.debug(logPrefix + "updateSqlTime");
        logPrefix = indent(logPrefix);

        PreparedStatement ps = null;
        final String logSuffix = " for dsName=" + dsName;
        try {
            ps = prepareStmt(logPrefix, c, update);
            bindUpdateParams(logPrefix, ps, time, dsName, sql);
            SafeSqlExecutor safeSqlExecutor = createExecutor();
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

    private void bindUpdateParams(String logPrefix, PreparedStatement ps, long time, String dsName, String sql)
            throws SQLException {
        int i = 1;
        ResultColumn.timeNanos.bind(ps, time, i++);
        ResultColumn.ts.bind(ps, new Timestamp(new Date().getTime()), i++);
        ResultColumn.host.bind(ps, host, i++);
        ResultColumn.logPrefix.bind(ps, logPrefix, i++);
        ResultColumn.ds.bind(ps, dsName, i++);
        ResultColumn.sql.bind(ps, sql, i++);
        ResultColumn.connTimeout.bind(ps, connTimeoutMillis, i++);
        ResultColumn.sqlExecTimeout.bind(ps, sqlExecTimeout, i++);

    }

    private void insertSqlTime(String logPrefix, Connection c, String dsName, long time, String sql) {
        logger.debug(logPrefix + "insertSqlTime");
        logPrefix = indent(logPrefix);

        PreparedStatement ps = null;
        try {
            ps = prepareStmt(logPrefix, c, insert);
            bindInsertParams(logPrefix, ps, time, dsName, sql);
            execute(logPrefix, ps, false, insert);
            logger.debug(logPrefix + "inserted result " + time);
        } catch (Exception e) {
            logger.error(logPrefix + "failed to insert result " + time, e);
        } finally {
            close(logPrefix, null, ps, null);
        }
    }

    private void bindInsertParams(String logPrefix, PreparedStatement ps, long time, String dsName, String sql)
            throws SQLException {
        ResultColumn.ds.bind(ps, dsName);
        ResultColumn.sql.bind(ps, sql);
        ResultColumn.connTimeout.bind(ps, connTimeoutMillis);
        ResultColumn.sqlExecTimeout.bind(ps, sqlExecTimeout);
        ResultColumn.timeNanos.bind(ps, time);
        ResultColumn.host.bind(ps, host);
        ResultColumn.logPrefix.bind(ps, logPrefix);
        ResultColumn.ts.bind(ps, new Timestamp(new Date().getTime()));
    }

    void close(String logPrefix, ResultSet rs, PreparedStatement ps, Connection c) {
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

    public int getConnTimeoutMillis() {
        return connTimeoutMillis;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public ExecutorService getExternalExecutor() {
        return externalExecutor;
    }

    public int getSqlExecTimeout() {
        return sqlExecTimeout;
    }

    public int getSqlExecTimeoutForcingPeriodMillis() {
        return sqlExecTimeoutForcingPeriodMillis;
    }

    public String getHost() {
        return host;
    }
}
