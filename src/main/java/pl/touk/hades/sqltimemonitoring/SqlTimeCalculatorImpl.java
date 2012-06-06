/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sqltimemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.sql.CancellableConnectionGetter;
import pl.touk.hades.exception.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.io.Serializable;

/**
 * TODO: description
 *
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class SqlTimeCalculatorImpl implements SqlTimeCalculator, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SqlTimeCalculatorImpl.class);

    private String sql = "select 1 from dual";
    private int sqlExecutionTimeout = -1;
    private int sqlExecutionTimeoutForcingPeriodMillis = -1;
    private int connectionGettingTimeout = -1;
    private int borrowExistingMatchingResultIfYoungerThanMillis = 0;

    private SqlTimeRepo sqlTimeRepo;

    public long calculateSqlTimeNanos(Hades hades, String logPrefix, boolean failover) throws InterruptedException {
        String dsName = failover ? hades.getFailoverDataSourceName() : hades.getMainDataSourceName();
        Long time = sqlTimeRepo.findSqlTimeYoungerThan(logPrefix, dsName, sql, sqlExecutionTimeout, sqlExecutionTimeoutForcingPeriodMillis, connectionGettingTimeout, borrowExistingMatchingResultIfYoungerThanMillis);
        if (time != null) {
            return time;
        }

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection(hades, failover, logPrefix);
            preparedStatement = prepareStatement(hades, connection, failover, logPrefix);
            time = execute(hades, preparedStatement, failover, logPrefix);
            return store(logPrefix, dsName, time);
        } catch (RuntimeException e) {
            logger.error(logPrefix + "unexpected runtime exception while preparing to measure statement execution time on " + hades.desc(failover), e);
            return store(logPrefix, dsName, SqlTimeBasedHadesLoadFactory.Error.unexpectedException.value());
        } catch (LocalLoadMeasuringException e) {
            return store(logPrefix, dsName, SqlTimeBasedHadesLoadFactory.Error.valueForException(e));
        } finally {
            close(hades, logPrefix, preparedStatement, connection, failover);
        }
    }

    private long store(String curRunLogPrefix, String dsName, Long time) {
        return sqlTimeRepo.storeSqlTime(curRunLogPrefix, dsName, sql, sqlExecutionTimeout, sqlExecutionTimeoutForcingPeriodMillis, connectionGettingTimeout, time);
    }

    private long execute(Hades haDataSource, PreparedStatement preparedStatement, boolean failover, String curRunLogPrefix) throws InterruptedException {
        if (sqlExecutionTimeout > 0 && sqlExecutionTimeoutForcingPeriodMillis >= 0) {
            return executeWithTimeout(haDataSource, preparedStatement, failover, curRunLogPrefix);
        } else {
            return executeWithoutTimeout(haDataSource, preparedStatement, failover, curRunLogPrefix);
        }
    }

    private long executeWithoutTimeout(Hades haDataSource, PreparedStatement preparedStatement, boolean failover, String curRunLogPrefix) {
        long start = System.nanoTime();
        try {
            preparedStatement.execute();
            return System.nanoTime() - start;
        } catch (SQLException e) {
            return handlePreparedStatementExecuteException(haDataSource, e, start, failover, curRunLogPrefix);
        } catch (RuntimeException e) {
            return handlePreparedStatementExecuteException(haDataSource, e, start, failover, curRunLogPrefix);
        }
    }

    private long handlePreparedStatementExecuteException(Hades haDataSource, Exception e, long start, boolean failover, String curRunLogPrefix) {
        long duration = System.nanoTime() - start;
        logger.error(curRunLogPrefix + "exception while measuring statement execution time on " + haDataSource.desc(failover) + " caught in " + SqlTimeBasedTriggerImpl.nanosToMillisHumanReadable(duration) + " ms", e);
        return Long.MAX_VALUE;
    }

    private long executeWithTimeout(final Hades haDataSource, final PreparedStatement preparedStatement, final boolean failover, final String curRunLogPrefix) throws InterruptedException {
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<Long> future = executor.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    return executeWithoutTimeout(haDataSource, preparedStatement, failover, curRunLogPrefix);
                }
            });
            return execute(haDataSource, future, failover, curRunLogPrefix);
        } catch (RuntimeException e) {
            logger.error(curRunLogPrefix + "unexpected runtime exception while executing statement on " + haDataSource.desc(failover) + "; assuming that the data source is unavailable", e.getCause());
            return Long.MAX_VALUE;
        }
    }

    private long execute(Hades haDataSource, Future<Long> future, boolean failover, String curRunLogPrefix) throws InterruptedException {
        try {
            return future.get(forcedStatementExecutionTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            logger.error(curRunLogPrefix + "unexpected exception while invoking future.get to executing statement on " + haDataSource.desc(failover) + "; assuming that the data source is unavailable", e.getCause());
            return Long.MAX_VALUE;
        } catch (TimeoutException e) {
            logger.error(curRunLogPrefix + "could not execute statement on " + haDataSource.desc(failover) + " within " + forcedStatementExecutionTimeoutMillis() + " millisecond(s); assuming that the data source is unavailable", e);
            future.cancel(false);
            return Long.MAX_VALUE;
        } catch (InterruptedException e) {
            logger.info(curRunLogPrefix + "interrupted while invoking future.get to execute statement on " + haDataSource.desc(failover) + "; interrupting statement execution", e);
            future.cancel(true);
            throw e;
        } catch (RuntimeException e) {
            logger.error(curRunLogPrefix + "unexpected runtime exception while invoking future.get to execute statement on " + haDataSource.desc(failover) + "; assuming that the data source is unavailable", e);
            future.cancel(false);
            return Long.MAX_VALUE;
        }
    }

    private int forcedStatementExecutionTimeoutMillis() {
        return sqlExecutionTimeout * 1000 + sqlExecutionTimeoutForcingPeriodMillis;
    }

    private PreparedStatement prepareStatement(Hades haDataSource, Connection connection, boolean failover, String curRunLogPrefix) throws StmtPrepareException, StmtQueryTimeoutSettingException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            if (sqlExecutionTimeout >= 0) {
                ps.setQueryTimeout(sqlExecutionTimeout);
            }
            return ps;
        } catch (SQLException e) {
            logger.error(curRunLogPrefix + "exception while " + (ps == null ? "preparing statement" : "setting query timeout") + " before measuring execution time on " + haDataSource.desc(failover), e);
            if (ps == null) {
                throw new StmtPrepareException(curRunLogPrefix);
            } else {
                throw new StmtQueryTimeoutSettingException(curRunLogPrefix);
            }
        }
    }

    private Connection getConnection(Hades haDataSource, final boolean failover, final String curRunLogPrefix) throws InterruptedException, ConnectionGettingException, ConnectionGettingTimeout {
        if (connectionGettingTimeout > 0) {
            return getConnectionWithTimeout(haDataSource, failover, curRunLogPrefix);
        } else {
            return haDataSource.getConnection(failover, curRunLogPrefix);
        }
    }

    private Connection getConnectionWithTimeout(Hades haDataSource, final boolean failover, final String curRunLogPrefix) throws InterruptedException, ConnectionGettingTimeout, ConnectionGettingException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CancellableConnectionGetter connectionGetter = new CancellableConnectionGetter(haDataSource, curRunLogPrefix, failover);
        Future<Connection> future = executor.submit(connectionGetter);
        return getConnection(haDataSource, future, connectionGetter, failover, curRunLogPrefix);
    }

    private Connection getConnection(Hades haDataSource, Future<Connection> future, CancellableConnectionGetter connectionGetter, boolean failover, String curRunLogPrefix) throws InterruptedException, ConnectionGettingTimeout, ConnectionGettingException {
        try {
            return future.get(connectionGettingTimeout, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return handleConnectionGettingException(curRunLogPrefix, haDataSource, failover, e.getCause());
        } catch (TimeoutException e) {
            logger.error(curRunLogPrefix + "could not get connection to " + haDataSource.desc(failover) + " within " + connectionGettingTimeout + " second(s); assuming that the data source is unavailable; cancelling connection creation", e);
            connectionGetter.cancel();
            // TODO: jeśli getConnection pobierają co najmniej dwa wątki, to tu powinno być future.cancel(true) - do zastanowienia, czy faktycznie tak by było lepiej
            future.cancel(false);
            throw new ConnectionGettingTimeout(curRunLogPrefix);
        } catch (InterruptedException e) {
            logger.info(curRunLogPrefix + "interrupted while getting (with timeout) connection to " + haDataSource.desc(failover) + "; interrupting connection creation", e);
            connectionGetter.cancel();
            future.cancel(true);
            throw e;
        } catch (RuntimeException e) {
            connectionGetter.cancel();
            future.cancel(false);
            throw new RuntimeException(curRunLogPrefix + "unexpected runtime exception while invoking future.get to get connection to " + haDataSource.desc(failover), e);
        }
    }

    private Connection handleConnectionGettingException(String curRunLogPrefix, Hades haDataSource, final boolean failover, Throwable t) throws ConnectionGettingException {
        if (t instanceof ConnectionGettingException) {
            throw (ConnectionGettingException) t;
        }
        String s = curRunLogPrefix + "unexpected exception while getting (with timeout) connection to " + haDataSource.desc(failover);
        logger.error(s, t);
        throw new RuntimeException(s, t);
    }

    private void close(Hades haDataSource, String curRunLogPrefix, PreparedStatement preparedStatement, Connection connection, boolean failover) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                logger.error(curRunLogPrefix + "exception while closing prepared statement for " + haDataSource.desc(failover) + " after measuring the execution time", e);
            } catch (RuntimeException e) {
                logger.error(curRunLogPrefix + "runtime exception while closing prepared statement for " + haDataSource.desc(failover) + " after measuring the execution time", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error(curRunLogPrefix + "exception while closing connection to " + haDataSource.desc(failover) + " after measuring the execution time", e);
            } catch (RuntimeException e) {
                logger.error(curRunLogPrefix + "runtime exception while closing connection to " + haDataSource.desc(failover) + " after measuring the execution time", e);
            }
        }
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSqlExecutionTimeout(int sqlExecutionTimeout) {
        this.sqlExecutionTimeout = sqlExecutionTimeout;
    }

    public void setSqlExecutionTimeoutForcingPeriodMillis(int sqlExecutionTimeoutForcingPeriodMillis) {
        this.sqlExecutionTimeoutForcingPeriodMillis = sqlExecutionTimeoutForcingPeriodMillis;
    }

    public void setConnectionGettingTimeout(int connectionGettingTimeout) {
        this.connectionGettingTimeout = connectionGettingTimeout;
    }

    public void setBorrowExistingMatchingResultIfYoungerThanMillis(int borrowExistingMatchingResultIfYoungerThanMillis) {
        this.borrowExistingMatchingResultIfYoungerThanMillis = borrowExistingMatchingResultIfYoungerThanMillis;
    }

    public void setSqlTimeRepo(SqlTimeRepo sqlTimeRepo) {
        this.sqlTimeRepo = sqlTimeRepo;
    }
}
