package pl.touk.hades.sql.timemonitoring;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public interface SqlTimeCalculator {
    long[] calculateMainAndFailoverSqlTimesNanos(String logPrefix, State state) throws InterruptedException;

    long estimateMaxExecutionTimeMillisOfCalculationMethod();

    String getLog(State oldState, State newState);
}
