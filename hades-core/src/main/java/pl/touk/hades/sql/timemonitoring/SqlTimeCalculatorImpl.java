/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql.timemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;
import pl.touk.hades.sql.HadesSafeConnectionGetter;
import pl.touk.hades.sql.SafeSqlExecutor;
import pl.touk.hades.exception.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.io.Serializable;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class SqlTimeCalculatorImpl implements SqlTimeCalculator, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SqlTimeCalculatorImpl.class);

    private final Hades hades;

    private final String sql;
    private final int sqlExecTimeout;
    private final int sqlExecTimeoutForcingPeriodMillis;
    private final int connTimeoutMillis;

    private final ExecutorService externalExecutor;

    private final Repo repo;

    public SqlTimeCalculatorImpl(Hades hades,
                                 int connTimeoutMillis,
                                 ExecutorService externalExecutor,
                                 Repo repo,
                                 String sql,
                                 int sqlExecTimeout,
                                 int sqlExecTimeoutForcingPeriodMillis) {
        Utils.assertNotNull(hades, "hades");
        Utils.assertNonNegative(sqlExecTimeout, "sqlExecTimeout");
        Utils.assertNonNegative(sqlExecTimeoutForcingPeriodMillis, "sqlExecTimeoutForcingPeriodMillis");
        Utils.assertNonNegative(connTimeoutMillis, "connTimeoutMillis");

        this.hades = hades;
        this.connTimeoutMillis = connTimeoutMillis;
        this.externalExecutor = externalExecutor;
        this.repo = repo;
        this.sql = sql;
        this.sqlExecTimeout = sqlExecTimeout;
        this.sqlExecTimeoutForcingPeriodMillis = sqlExecTimeoutForcingPeriodMillis;
    }

    public SqlTimeCalculatorImpl(Hades hades, Repo repo, ExecutorService executor) {
        this.hades = hades;
        this.connTimeoutMillis = 0;
        this.externalExecutor = executor;
        this.repo = repo;
        this.sql = "select sysdate from dual";
        this.sqlExecTimeout = 0;
        this.sqlExecTimeoutForcingPeriodMillis = 0;
    }

    public long[] calculateMainAndFailoverSqlTimesNanos(final MonitorRunLogPrefix logPrefix, final State state)
            throws InterruptedException {
        logger.debug(logPrefix + "calculateMainAndFailoverSqlTimesNanos");
        MonitorRunLogPrefix indentedLogPrefix = logPrefix.indent();
        ExecutorService executor = null;
        try {
            if (externalExecutor != null) {
                executor = externalExecutor;
            } else {
                executor = Executors.newFixedThreadPool(2);
            }
            List<Future<Long>> futures = executor.invokeAll(Arrays.asList(
                    createCallable(indentedLogPrefix, state, false),
                    createCallable(indentedLogPrefix, state, true)));
            return new long[]{
                    extractSqlTime(indentedLogPrefix, futures, false),
                    extractSqlTime(indentedLogPrefix, futures, true)};
        } catch (RuntimeException e) {
            logger.error(indentedLogPrefix + "unexpected RuntimeException while trying to measure load on main and " +
                    "failover data sources concurrently; assuming that both data sources are unavailable", e);
            return new long[]{ExceptionEnum.unexpectedException.value(), ExceptionEnum.unexpectedException.value()};
        } finally {
            if (externalExecutor == null && executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private Callable<Long> createCallable(final MonitorRunLogPrefix logPrefix,
                                          final State state,
                                          final boolean failover) {
        return new Callable<Long>() {
            public Long call() throws Exception {
                return calculateSqlTimeNanos(logPrefix.append(hades.getDsName(failover, true) + ": "), state, failover);
            }
        };
    }

    private long extractSqlTime(MonitorRunLogPrefix logPrefix, List<Future<Long>> futures, boolean failover)
            throws InterruptedException {
        try {
            return futures.get(failover ? 1 : 0).get();
        } catch (ExecutionException e) {
            logger.error(logPrefix + "unexpected exception while getting sql time for " +
                    (failover ? "failover" : "main") + " database from java.util.concurrent.Future", e.getCause());
            return ExceptionEnum.unexpectedException.value();
        } catch (InterruptedException e) {
            futures.get(0).cancel(true);
            futures.get(1).cancel(true);
            throw e;
        }
    }

    private long calculateSqlTimeNanos(MonitorRunLogPrefix logPrefix, State state, boolean failover)
            throws InterruptedException {
        logger.debug(logPrefix + "calculateSqlTimeNanos");
        logPrefix = logPrefix.indent();

        String dsName = hades.getDsName(failover);
        Long time;
        if (state.sqlTimeIsMeasuredInThisCycle(logPrefix, failover)) {
            time = repo.findSqlTimeYoungerThan(logPrefix, dsName, sql);
        } else {
            return State.notMeasuredInThisCycle;
        }

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection(failover, logPrefix);
            if (time != null) {
                return time;
            }
            preparedStatement =
                    Utils.safelyPrepareStatement(logPrefix, connection, hades.desc(failover), sqlExecTimeout, sql);
            time = new SafeSqlExecutor(
                    sqlExecTimeout,
                    sqlExecTimeoutForcingPeriodMillis,
                    hades.desc(failover),
                    externalExecutor
            ).execute(logPrefix, preparedStatement, false, sql);
            return repo.storeSqlTime(logPrefix, hades, dsName, time, sql);
        } catch (LoadMeasuringException e) {
            return repo.storeException(logPrefix, hades, dsName, e, sql);
        } catch (RuntimeException e) {
            logger.error(logPrefix + "unexpected runtime exception while measuring statement execution time on " +
                    hades.desc(failover), e);
            return repo.storeException(logPrefix, hades, dsName, e, sql);
        } finally {
            close(logPrefix, hades, preparedStatement, connection, failover);
        }
    }

    public Connection getConnection(final boolean failover, final MonitorRunLogPrefix curRunLogPrefix)
            throws InterruptedException, LoadMeasuringException {
        if (connTimeoutMillis > 0) {
            return new HadesSafeConnectionGetter(
                    connTimeoutMillis,
                    hades,
                    failover,
                    externalExecutor
            ).getConnectionWithTimeout(curRunLogPrefix);
        } else {
            return hades.getConnection(curRunLogPrefix, failover);
        }
    }

    public long estimateMaxExecutionTimeMillisOfCalculationMethod() {
        return (connTimeoutMillis > 0 ? connTimeoutMillis : 100)
                + (sqlExecTimeout > 0 ? sqlExecTimeout * 1000 + sqlExecTimeoutForcingPeriodMillis: 1000);
    }

    public void close(MonitorRunLogPrefix logPrefix,
                      Hades hades,
                      PreparedStatement ps,
                      Connection c,
                      boolean failover) {
        if (ps != null) {
            try {
                ps.close();
            } catch (Exception e) {
                logger.error(logPrefix + "exception while closing prepared statement for " + hades.desc(failover), e);
            }
        }
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                logger.error(logPrefix + "exception while closing connection to " + hades.desc(failover), e);
            }
        }
    }

    public String getLog(State oldState, State newState) {
        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
        return "average execution time for '" + sql + "' for last " + newState.getAvg().getItemsCountIncludedInAverage()
                + " execution(s): " + Utils.nanosToMillisAsStr(newState.getAvg().getValue())
                + " (last: " + Utils.nanosToMillisAsStr(newState.getAvg().getLast()) + ") - " + hades.getMainDsName()
                + ", " + Utils.nanosToMillisAsStr(newState.getAvgFailover().getValue())
                + " (last: " + Utils.nanosToMillisAsStr(newState.getAvgFailover().getLast()) + ") - "
                + hades.getFailoverDsName() + "; load levels derived from average execution times: "
                + getLoadLevels(newState) + "; " + createTransitionDesc(oldState, newState)
                + (failoverDataSourcePinned != null ? "; above calculations currently have no effect because "
                + (failoverDataSourcePinned ? "failover data source (" + hades.getFailoverDsName()
                + ")" : "main data source (" + hades.getMainDsName() + ")") + " is pinned and therefore used" : "");
    }

    private String getLoadLevels(State state) {
        Load load = state.getMachineState().getLoad();
        return load.getMainDb()     + " - " + hades.getMainDsName() + ", " +
               load.getFailoverDb() + " - " + hades.getFailoverDsName() +
               (load.isMainDbLoadHigher() != null ?
                       " (" + hades.getMainDsName() + " load level is" +
                               (load.isMainDbLoadHigher() ? "" : " not") + " higher)" :
                       "");
    }

    private String createTransitionDesc(State oldState, State newState) {
        boolean failoverOrFailback =
                newState.getMachineState().isFailoverActive() != oldState.getMachineState().isFailoverActive();
        String dbSwitchType;
        if (failoverOrFailback) {
            dbSwitchType = newState.getMachineState().isFailoverActive() ?
                    "activating failover (" + hades.getMainDsName()     + " -> " + hades.getFailoverDsName() + ")" :
                    "activating failback (" + hades.getFailoverDsName() + " -> " + hades.getMainDsName()     + ")";
        } else {
            if (newState.getMachineState().isFailoverActive()) {
                dbSwitchType = "failover remains active (keep using " + hades.getFailoverDsName() + ")";
            } else {
                dbSwitchType = "failover remains inactive (keep using " + hades.getMainDsName() + ")";
            }
        }
        return dbSwitchType;
    }
}
