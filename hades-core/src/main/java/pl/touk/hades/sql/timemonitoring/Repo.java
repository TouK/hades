package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Hades;

public interface Repo {
    Long findSqlTimeYoungerThan(MonitorRunLogPrefix curRunLogPrefix, String dsName, String sql)
            throws InterruptedException;

    long storeSqlTime(MonitorRunLogPrefix curRunLogPrefix, Hades hades, String dsName, long time, String sql)
            throws InterruptedException;

    long storeException(MonitorRunLogPrefix curRunLogPrefix, Hades hades, String dsName, Exception e, String sql)
            throws InterruptedException;

    String getHost();

    String getRepoId();
}
