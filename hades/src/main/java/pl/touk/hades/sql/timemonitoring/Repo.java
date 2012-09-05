package pl.touk.hades.sql.timemonitoring;

public interface Repo {
    Long findSqlTimeYoungerThan(String curRunLogPrefix, String dsName) throws InterruptedException;

    long storeSqlTime(String curRunLogPrefix, String dsName, long time) throws InterruptedException;

    long storeException(String curRunLogPrefix, String dsName, Exception e) throws InterruptedException;

    String getHost();
}
