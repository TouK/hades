package pl.touk.hades.sql.timemonitoring.repo;

import pl.touk.hades.sql.timemonitoring.SqlTimeCalculator;
import pl.touk.hades.sql.timemonitoring.State;

import java.util.Date;

public interface SqlTimeRepo {
    Long findSqlTimeYoungerThan(String curRunLogPrefix, String dsName) throws InterruptedException;

    long storeSqlTime(String curRunLogPrefix, String dsName, long time) throws InterruptedException;

    long storeException(String curRunLogPrefix, String dsName, Exception e) throws InterruptedException;

    void init(SqlTimeCalculator sqlTimeCalculator);

    State getHadesClusterState(String curSyncLogPrefix, long lowerBound, long[] measuringDurationMillis) throws InterruptedException;

    void saveHadesClusterState(String curRunLogPrefix, State state, long runMethodStartMillis) throws InterruptedException;
}
