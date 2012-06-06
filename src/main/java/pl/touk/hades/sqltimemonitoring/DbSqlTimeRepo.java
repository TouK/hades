package pl.touk.hades.sqltimemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;

public class DbSqlTimeRepo implements SqlTimeRepo {

    private static final Logger logger = LoggerFactory.getLogger(DbSqlTimeRepo.class);

    public static final String table = "HADES_RESULTS";

    public static enum Column {
        ds                                ("DS"                   , Types.VARCHAR),
        sql                               ("SQL_STMT"             , Types.VARCHAR),
        sqlExecTimeout                    ("EXEC_TIMEOUT"         , Types.INTEGER),
        sqlExecTimeoutForcingPeriodMillis ("FORCING_PERIOD_MILLIS", Types.INTEGER),
        connectionGettingTimeout          ("CONN_TIMEOUT"         , Types.INTEGER),
        time                              ("TIME"                 , Types.INTEGER),
        instanceId                        ("INSTANCE_ID"          , Types.VARCHAR),
        logPrefix                         ("LOG_PREFIX"           , Types.VARCHAR),
        ts                                ("TS"                   , Types.TIMESTAMP);

        private String name;
        private int type;

        Column(String name, int type) {
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

    public static final String columnList = Column.list();
    public static final String questionMarkList = Column.questionMarkList();

    private static final String insert = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + questionMarkList + ")";
    private static final String select =
            "SELECT " +
                    Column.time + ", " +
                    Column.sqlExecTimeout + ", " +
                    Column.sqlExecTimeoutForcingPeriodMillis + ", " +
                    Column.connectionGettingTimeout + ", " +
                    Column.instanceId + ", " +
                    Column.logPrefix +
            " FROM " + table +
            " WHERE " + Column.ts + " > ? " +
            "  AND " + Column.ds + " = ?" +
            "  AND " + Column.sql + " = ?";

    private DataSource dataSource;
    // TODO
    private String quartzInstanceId = "TODO";

    public Long findSqlTimeYoungerThan(String curRunLogPrefix, String dsName, String sql, int sqlExecutionTimeout, int sqlExecutionTimeoutForcingPeriodMillis, int connectionGettingTimeout, int youngerThanMillis) {
        Connection c = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            c = dataSource.getConnection();
            ps = prepareStatement(c, youngerThanMillis,  dsName, sql);
            ps.execute();
            rs = ps.getResultSet();
            if (rs.next()) {
                long time = rs.getLong(Column.time.name);
                int sqlExecutionTimeoutFromDb = rs.getInt(Column.time.name);
                int sqlExecutionTimeoutForcingPeriodMillisFromDb = rs.getInt(Column.time.name);
                int connectionGettingTimeoutFromDb = rs.getInt(Column.time.name);
                String quartzInstanceId = rs.getString(Column.instanceId.name);
                String logPrefix = rs.getString(Column.logPrefix.name);
                // TODO: czy timeouty ok
                logger.info(curRunLogPrefix + "borrowing from " + logPrefix + " on " + quartzInstanceId + " result " + SqlTimeBasedHadesLoadFactory.Error.erroneousValuesAsStr(time) + " for parameters: dsName=" + dsName + ", sql=" + sql + ", sqlExecutionTimeout=" + sqlExecutionTimeout + ", sqlExecutionTimeoutForcingPeriodMillis=" + sqlExecutionTimeoutForcingPeriodMillis + ", connectionGettingTimeout=" + connectionGettingTimeout);
                return time;
            } else {
                return null;
            }
        } catch (SQLException e) {
            logger.error(curRunLogPrefix + "failed to borrow sql time in db for parameters: dsName=" + dsName + ", sql=" + sql + ", sqlExecutionTimeout=" + sqlExecutionTimeout + ", sqlExecutionTimeoutForcingPeriodMillis=" + sqlExecutionTimeoutForcingPeriodMillis + ", connectionGettingTimeout=" + connectionGettingTimeout, e);
            return null;
        } finally {
            close(rs, ps, c);
        }
    }

    private PreparedStatement prepareStatement(Connection c, int youngerThanMillis, String dsName, String sql) throws SQLException {
        PreparedStatement ps = c.prepareStatement(select);
        Column.ts.bind(ps, new Timestamp(new Date().getTime() - youngerThanMillis), 1);
        Column.ds.bind(ps, dsName, 2);
        Column.ds.bind(ps, sql, 3);
        return ps;
    }

    public long storeSqlTime(String logPrefix, String dsName, String sql, int sqlExecutionTimeout, int sqlExecutionTimeoutForcingPeriodMillis, int connectionGettingTimeout, long time) {
        Connection c = null;
        PreparedStatement ps = null;
        try {
            c = dataSource.getConnection();
            c.setAutoCommit(true);
            ps = c.prepareStatement(insert);

            Column.ds.bind(ps, dsName);
            Column.connectionGettingTimeout.bind(ps, connectionGettingTimeout);
            Column.sql.bind(ps, sql);
            Column.sqlExecTimeout.bind(ps, sqlExecutionTimeout);
            Column.sqlExecTimeoutForcingPeriodMillis.bind(ps, sqlExecutionTimeoutForcingPeriodMillis);
            Column.time.bind(ps, time);
            Column.instanceId.bind(ps, quartzInstanceId);
            Column.logPrefix.bind(ps, logPrefix);
            Column.ts.bind(ps, new Timestamp(new Date().getTime()));

            ps.execute();
            logger.debug(logPrefix + "stored result " + time + " for parameters: dsName=" + dsName + ", sql=" + sql + ", sqlExecutionTimeout=" + sqlExecutionTimeout + ", sqlExecutionTimeoutForcingPeriodMillis=" + sqlExecutionTimeoutForcingPeriodMillis + ", connectionGettingTimeout=" + connectionGettingTimeout);
        } catch (SQLException e) {
            logger.error(logPrefix + "failed to store sql time in db: dsName=" + dsName + ", sql=" + sql + ", sqlExecutionTimeout=" + sqlExecutionTimeout + ", sqlExecutionTimeoutForcingPeriodMillis=" + sqlExecutionTimeoutForcingPeriodMillis + ", connectionGettingTimeout=" + connectionGettingTimeout, e);
        } finally {
            close(null, ps, c);
        }
        return time;
    }

    private void close(ResultSet rs, PreparedStatement ps, Connection c) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
            }
        }
        if (c != null) {
            try {
                c.close();
            } catch (SQLException e) {
            }
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
