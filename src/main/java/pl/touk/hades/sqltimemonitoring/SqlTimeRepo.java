package pl.touk.hades.sqltimemonitoring;

public interface SqlTimeRepo {
    Long findSqlTimeYoungerThan(String curRunLogPrefix, String dsName, String sql, int sqlExecutionTimeout, int sqlExecutionTimeoutForcingPeriodMillis, int connectionGettingTimeout, int youngerThanMillis);
    long storeSqlTime(String curRunLogPrefix, String dsName, String sql, int sqlExecutionTimeout, int sqlExecutionTimeoutForcingPeriodMillis, int connectionGettingTimeout, long time);
}
