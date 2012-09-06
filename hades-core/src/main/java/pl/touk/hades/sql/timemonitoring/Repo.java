package pl.touk.hades.sql.timemonitoring;

public interface Repo {
    Long findSqlTimeYoungerThan(String curRunLogPrefix, String dsName, String sql) throws InterruptedException;

    long storeSqlTime(String curRunLogPrefix, String dsName, long time, String sql) throws InterruptedException;

    long storeException(String curRunLogPrefix, String dsName, Exception e, String sql) throws InterruptedException;

    String getHost();
}
