/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql.timemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.sql.HadesSafeConnectionGetter;
import pl.touk.hades.sql.SafeSqlExecutor;
import pl.touk.hades.sql.timemonitoring.repo.SqlTimeRepo;
import pl.touk.hades.exception.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.io.Serializable;

import static pl.touk.hades.Utils.indent;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class SqlTimeCalculatorImpl implements SqlTimeCalculator, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SqlTimeCalculatorImpl.class);

    private String sql = "select 1 from dual";
    private int sqlExecTimeout;
    private int sqlExecTimeoutForcingPeriodMillis;
    private int connTimeoutMillis;

    private ExecutorService externalExecutor;

    private SqlTimeRepo sqlTimeRepo;

    private SqlTimeBasedTrigger sqlTimeBasedTrigger;

    public SqlTimeCalculatorImpl() {
        setSqlExecTimeout(0);
        setSqlExecTimeoutForcingPeriodMillis(0);
        setConnTimeoutMillis(0);
    }

    public void init(SqlTimeBasedTrigger sqlTimeBasedTrigger) {
        Utils.assertNotNull(sqlTimeBasedTrigger, "sqlTimeBasedTrigger");
        Utils.assertSame(sqlTimeBasedTrigger.getSqlTimeCalculator(), this, "sqlTimeBasedTrigger.sqlTimeCalculator != this");
        Utils.assertNull(this.sqlTimeBasedTrigger, "sqlTimeBasedTrigger; ensure that each sqlTimeBasedTrigger has its own SqlTimeCalculator");

        this.sqlTimeBasedTrigger = sqlTimeBasedTrigger;

        sqlTimeRepo.init(this);
    }

    public long[] calculateMainAndFailoverSqlTimesNanos(final String logPrefix, final State state) throws InterruptedException {
        logger.debug(logPrefix + "calculateMainAndFailoverSqlTimesNanos");
        ExecutorService executor = null;
        try {
            if (externalExecutor != null) {
                executor = externalExecutor;
            } else {
                executor = Executors.newFixedThreadPool(2);
            }
            List<Future<Long>> futures = executor.invokeAll(Arrays.asList(createCallable(indent(logPrefix), state, false), createCallable(indent(logPrefix), state, true)));
            return new long[]{extractSqlTime(indent(logPrefix), futures, false), extractSqlTime(indent(logPrefix), futures, true)};
        } catch (RuntimeException e) {
            logger.error(indent(logPrefix) + "unexpected RuntimeException while trying to measure load on main and failover data sources concurrently; assuming that both data sources are unavailable", e);
            return new long[]{ExceptionEnum.unexpectedException.value(), ExceptionEnum.unexpectedException.value()};
        } finally {
            if (externalExecutor == null && executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private Callable<Long> createCallable(final String logPrefix, final State state, final boolean failover) {
        return new Callable<Long>() {
            public Long call() throws Exception {
                return calculateSqlTimeNanos(logPrefix + getHades().getDsName(failover, true) + ": ", state, failover);
            }
        };
    }

    private long extractSqlTime(String logPrefix, List<Future<Long>> futures, boolean failover) throws InterruptedException {
        try {
            return futures.get(failover ? 1 : 0).get();
        } catch (ExecutionException e) {
            logger.error(logPrefix + "unexpected exception while getting sql time for " + (failover ? "failover" : "main") + " database from java.util.concurrent.Future", e.getCause());
            return ExceptionEnum.unexpectedException.value();
        } catch (InterruptedException e) {
            futures.get(0).cancel(true);
            futures.get(1).cancel(true);
            throw e;
        }
    }

    private long calculateSqlTimeNanos(String logPrefix, State state, boolean failover) throws InterruptedException {
        logger.debug(logPrefix + "calculateSqlTimeNanos");
        logPrefix = indent(logPrefix);

        String dsName = getHades().getDsName(failover);
        Long time;
        if (state.sqlTimeIsMeasuredInThisCycle(failover)) {
            time = sqlTimeRepo.findSqlTimeYoungerThan(logPrefix, dsName);
        } else {
            logger.debug(logPrefix + "not measured in this cycle (only getConnection is executed)");
            time = State.notMeasuredInThisCycle;
        }

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection(failover, logPrefix);
            if (time != null) {
                return time;
            }
            preparedStatement = Utils.safelyPrepareStatement(logPrefix, connection, getHades().desc(failover), sqlExecTimeout, sql);
            time = new SafeSqlExecutor(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, getHades().desc(failover), externalExecutor).execute(logPrefix, preparedStatement, false, sql);
            return sqlTimeRepo.storeSqlTime(logPrefix, dsName, time);
        } catch (LoadMeasuringException e) {
            return sqlTimeRepo.storeException(logPrefix, dsName, e);
        } catch (RuntimeException e) {
            logger.error(logPrefix + "unexpected runtime exception while measuring statement execution time on " + getHades().desc(failover), e);
            return sqlTimeRepo.storeException(logPrefix, dsName, e);
        } finally {
            close(getHades(), logPrefix, preparedStatement, connection, failover);
        }
    }

    private Connection getConnection(final boolean failover, final String curRunLogPrefix) throws InterruptedException, LoadMeasuringException {
        if (connTimeoutMillis > 0) {
            return new HadesSafeConnectionGetter(connTimeoutMillis, getHades(), failover, externalExecutor).getConnectionWithTimeout(curRunLogPrefix);
        } else {
            return getHades().getConnection(curRunLogPrefix, failover);
        }
    }

    public Hades getHades() {
        return sqlTimeBasedTrigger.getHades();
    }

    public long estimateMaxExecutionTimeMillisOfCalculationMethod() {
        return (connTimeoutMillis > 0 ? connTimeoutMillis : 100) + (sqlExecTimeout > 0 ? sqlExecTimeout * 1000 + sqlExecTimeoutForcingPeriodMillis: 1000);
    }

    public State syncValidate(String logPrefix, State state) throws InterruptedException {
        logger.debug(logPrefix + "syncValidate");
        logPrefix = indent(logPrefix);

        boolean failover = state.getMachineState().isFailoverActive();
        Connection c = null;
        try {
            c = getConnection(failover, logPrefix);
            logger.debug(logPrefix + "state is valid");
            return state;
        } catch (LoadMeasuringException e) {
            logger.warn(logPrefix + "borrowed " + state + " is invalid - can't get connection to " + getHades().getDsName(failover));
        } finally {
            close(getHades(), logPrefix, null, c, failover);
        }
        c = null;
        try {
            c = getConnection(!failover, logPrefix);
            logger.warn(logPrefix + "state is invalid but its reverted form perfectly is");
            return new State(
                    state.getQuartzInstanceId() + " (reverted by " + getSqlTimeBasedTrigger().getSchedulerInstanceId() + " while syncing)",
                    System.currentTimeMillis(),
                    !failover,
                    !failover ? ExceptionEnum.connException.value() : state.getAvg().getLast(),
                    failover ? ExceptionEnum.connException.value() : state.getAvgFailover().getLast());
        } catch (LoadMeasuringException e) {
            logger.warn(logPrefix + "state is invalid and so is its reverted form");
            return new State(
                    state.getQuartzInstanceId() + " (no ds could by connected from " + getSqlTimeBasedTrigger().getSchedulerInstanceId() + " while syncing)",
                    System.currentTimeMillis(),
                    false,
                    ExceptionEnum.connException.value(),
                    ExceptionEnum.connException.value());
        } finally {
            close(getHades(), logPrefix, null, c, !failover);
        }
    }

    private void close(Hades haDataSource, String curRunLogPrefix, PreparedStatement preparedStatement, Connection connection, boolean failover) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                logger.error(curRunLogPrefix + "exception while closing prepared statement for " + haDataSource.desc(failover), e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                logger.error(curRunLogPrefix + "exception while closing connection to " + haDataSource.desc(failover), e);
            }
        }
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSqlExecTimeout(int sqlExecTimeout) {
        Utils.assertNonNegative(sqlExecTimeout, "sqlExecTimeout");
        this.sqlExecTimeout = sqlExecTimeout;
    }

    public void setSqlExecTimeoutForcingPeriodMillis(int sqlExecTimeoutForcingPeriodMillis) {
        Utils.assertNonNegative(sqlExecTimeoutForcingPeriodMillis, "sqlExecTimeoutForcingPeriodMillis");
        this.sqlExecTimeoutForcingPeriodMillis = sqlExecTimeoutForcingPeriodMillis;
    }

    public void setConnTimeoutMillis(int connTimeoutMillis) {
        Utils.assertNonNegative(connTimeoutMillis, "connTimeoutMillis");
        this.connTimeoutMillis = connTimeoutMillis;
    }

    public void setSqlTimeRepo(SqlTimeRepo sqlTimeRepo) {
        this.sqlTimeRepo = sqlTimeRepo;
    }

    public SqlTimeBasedTrigger getSqlTimeBasedTrigger() {
        return sqlTimeBasedTrigger;
    }

    public SqlTimeRepo getSqlTimeRepo() {
        return sqlTimeRepo;
    }

    public int getSqlExecTimeout() {
        return sqlExecTimeout;
    }

    public int getConnTimeoutMillis() {
        return connTimeoutMillis;
    }

    public void setExternalExecutor(ExecutorService externalExecutor) {
        this.externalExecutor = externalExecutor;
    }
}
