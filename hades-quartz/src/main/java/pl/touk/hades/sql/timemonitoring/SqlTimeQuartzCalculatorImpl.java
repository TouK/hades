package pl.touk.hades.sql.timemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.exception.LoadMeasuringException;

import java.lang.InterruptedException;
import java.lang.String;
import java.lang.System;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class SqlTimeQuartzCalculatorImpl implements SqlTimeQuartzCalculator {

    private static final Logger logger = LoggerFactory.getLogger(SqlTimeCalculatorImpl.class);

    private final SqlTimeCalculatorImpl calc;
    private final Hades hades;
    private final Repo repo;

    public SqlTimeQuartzCalculatorImpl(Hades hades,
                                       int connTimeoutMillis,
                                       ExecutorService externalExecutor,
                                       QuartzRepo repo,
                                       String sql,
                                       int sqlExecTimeout,
                                       int sqlExecTimeoutForcingPeriodMillis) {
        Utils.assertNotNull(hades, "hades");
        Utils.assertNotNull(repo, "repo");

        this.calc = new SqlTimeCalculatorImpl(
                hades,
                connTimeoutMillis,
                externalExecutor,
                repo,
                sql,
                sqlExecTimeout,
                sqlExecTimeoutForcingPeriodMillis);
        this.hades = hades;
        this.repo = repo;
    }

    public SqlTimeQuartzCalculatorImpl(Hades hades) {
        this.hades = hades;
        this.calc = new SqlTimeCalculatorImpl(null, null, null);
        this.repo = null;
    }

    public long[] calculateMainAndFailoverSqlTimesNanos(MonitorRunLogPrefix logPrefix, State state)
            throws InterruptedException {
        return calc.calculateMainAndFailoverSqlTimesNanos(logPrefix, state);
    }

    public long estimateMaxExecutionTimeMillisOfCalculationMethod() {
        return calc.estimateMaxExecutionTimeMillisOfCalculationMethod();
    }

    public State syncValidate(MonitorRunLogPrefix logPrefix, State state) throws InterruptedException {
        logger.debug(logPrefix + "syncValidate");
        logPrefix = logPrefix.indent();

        boolean failover = state.getMachineState().isFailoverActive();
        Connection c = null;
        try {
            c = calc.getConnection(failover, logPrefix);
            logger.debug(logPrefix + "state is valid");
            return state;
        } catch (LoadMeasuringException e) {
            logger.warn(logPrefix + "borrowed " + state +
                    " is invalid - can't get connection to " + hades.getDsName(failover));
        } finally {
            calc.close(logPrefix, hades, null, c, failover);
        }
        c = null;
        try {
            c = calc.getConnection(!failover, logPrefix);
            logger.warn(logPrefix + "state is invalid but its reverted form perfectly is");
            return new State(
                    state.getHost() + " (reverted by " + repo.getHost() + " while syncing)",
                    state.getRepoId(),
                    System.currentTimeMillis(),
                    !failover,
                    !failover ? ExceptionEnum.connException.value() : state.getAvg().getLast(),
                    failover ? ExceptionEnum.connException.value() : state.getAvgFailover().getLast());
        } catch (LoadMeasuringException e) {
            logger.warn(logPrefix + "state is invalid and so is its reverted form");
            return new State(
                    state.getHost() + " (no ds could by connected from " + repo.getHost() + " while syncing)",
                    state.getRepoId(),
                    System.currentTimeMillis(),
                    false,
                    ExceptionEnum.connException.value(),
                    ExceptionEnum.connException.value());
        } finally {
            calc.close(logPrefix, hades, null, c, !failover);
        }
    }

    public String getLog(State oldState, State newState) {
        return calc.getLog(oldState, newState);
    }
}
