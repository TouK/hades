package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Hades;
import pl.touk.hades.exception.LoadMeasuringException;
import pl.touk.hades.sql.timemonitoring.repo.SqlTimeRepo;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public interface SqlTimeCalculator {
    long[] calculateMainAndFailoverSqlTimesNanos(String logPrefix, State state) throws InterruptedException;
    long estimateMaxExecutionTimeMillisOfCalculationMethod();
    String getSql();
    void init(SqlTimeBasedTrigger sqlTimeBasedTrigger);
    SqlTimeRepo getSqlTimeRepo();

    void setConnTimeoutMillis(int connTimeout);

    int getSqlExecTimeout();

    int getConnTimeoutMillis();

    Hades getHades();

    SqlTimeBasedTrigger getSqlTimeBasedTrigger();

    State syncValidate(String curSyncLogPrefix, State state) throws InterruptedException;
}
