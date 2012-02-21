/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.HaDataSource;

import java.text.DecimalFormat;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * TODO: description
 *
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class StmtExecTimeCalculatorImpl implements StmtExecTimeCalculator {

    private static final Logger logger = LoggerFactory.getLogger(StmtExecTimeCalculatorImpl.class);

    private static final DecimalFormat decimalFormat = new DecimalFormat("#0.000 ms");

    private String statement = "select 1 from dual";
    private int statementExecutionTimeout = -1;
    private int statementExecutionTimeoutForcingPeriodMillis = -1;
    private int connectionGettingTimeout = -1;

    public long calculateStmtExecTimeNanos(HaDataSource haDataSource, String curRunLogPrefix, boolean failover) throws InterruptedException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        long start = System.nanoTime();
        try {
            connection = getConnection(haDataSource, failover, curRunLogPrefix);
            preparedStatement = prepareStatement(haDataSource, connection, failover, curRunLogPrefix);
            if (preparedStatement == null) {
                return Long.MAX_VALUE;
            }
        } catch (RuntimeException e) {
            long exceptionMoment = System.nanoTime();
            logger.error(curRunLogPrefix + "runtime exception while preparing to measure statement execution time on " + haDataSource.desc(failover) + " caught in " + nanosToMillis(exceptionMoment - start) + " ms since the beginning of the preparation", e);
            close(haDataSource, curRunLogPrefix, preparedStatement, connection, failover);
            return Long.MAX_VALUE;
        }
        return execute(haDataSource, connection, preparedStatement, failover, curRunLogPrefix);
    }

    private long execute(HaDataSource haDataSource, Connection connection, PreparedStatement preparedStatement, boolean failover, String curRunLogPrefix) throws InterruptedException {
        if (statementExecutionTimeout > 0 && statementExecutionTimeoutForcingPeriodMillis >= 0) {
            return executeWithTimeout(haDataSource, connection, preparedStatement, failover, curRunLogPrefix);
        } else {
            return executeWithoutTimeout(haDataSource, connection, preparedStatement, failover, curRunLogPrefix);
        }
    }

    private long executeWithoutTimeout(HaDataSource haDataSource, Connection connection, PreparedStatement preparedStatement, boolean failover, String curRunLogPrefix) {
        long start = System.nanoTime();
        try {
            preparedStatement.execute();
            return System.nanoTime() - start;
        } catch (SQLException e) {
            return handlePreparedStatementExecuteException(haDataSource, e, start, failover, curRunLogPrefix);
        } catch (RuntimeException e) {
            return handlePreparedStatementExecuteException(haDataSource, e, start, failover, curRunLogPrefix);
        } finally {
            close(haDataSource, curRunLogPrefix, preparedStatement, connection, failover);
        }
    }

    private long handlePreparedStatementExecuteException(HaDataSource haDataSource, Exception e, long start, boolean failover, String curRunLogPrefix) {
        long duration = System.nanoTime() - start;
        logger.error(curRunLogPrefix + "exception while measuring statement execution time on " + haDataSource.desc(failover) + " caught in " + nanosToMillis(duration) + " ms", e);
        return Long.MAX_VALUE;
    }

    private long executeWithTimeout(final HaDataSource haDataSource, final Connection connection, final PreparedStatement preparedStatement, final boolean failover, final String curRunLogPrefix) throws InterruptedException {
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<Long> future = executor.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    return executeWithoutTimeout(haDataSource, connection, preparedStatement, failover, curRunLogPrefix);
                }
            });
            return execute(haDataSource, future, failover, curRunLogPrefix);
        } catch (RuntimeException e) {
            logger.error(curRunLogPrefix + "unexpected runtime exception while executing statement on " + haDataSource.desc(failover) + "; assuming that the data source is unavailable", e.getCause());
            return Long.MAX_VALUE;
        }
    }

    private long execute(HaDataSource haDataSource, Future<Long> future, boolean failover, String curRunLogPrefix) throws InterruptedException {
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
        return statementExecutionTimeout * 1000 + statementExecutionTimeoutForcingPeriodMillis;
    }

    private PreparedStatement prepareStatement(HaDataSource haDataSource, Connection connection, boolean failover, String curRunLogPrefix) {
        if (connection != null) {
            try {
                PreparedStatement s = connection.prepareStatement(statement);
                if (statementExecutionTimeout >= 0) {
                    s.setQueryTimeout(statementExecutionTimeout);
                }
                return s;
            } catch (SQLException e) {
                logger.error(curRunLogPrefix + "exception while preparing statement or setting query timeout for measuring execution time on " + haDataSource.desc(failover), e);
                return null;
            }
        } else {
            return null;
        }
    }

    private Connection getConnection(HaDataSource haDataSource, final boolean failover, final String curRunLogPrefix) throws InterruptedException {
        if (connectionGettingTimeout > 0) {
            return getConnectionWithTimeout(haDataSource, failover, curRunLogPrefix);
        } else {
            try {
                return haDataSource.getConnection(failover, false, null, null, curRunLogPrefix);
            } catch (SQLException e) {
                return null;
            }
        }
    }

    private Connection getConnectionWithTimeout(HaDataSource haDataSource, final boolean failover, final String curRunLogPrefix) throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CancellableConnectionGetter connectionGetter = new CancellableConnectionGetter(haDataSource, curRunLogPrefix, failover);
        Future<Connection> future = executor.submit(connectionGetter);
        return getConnection(haDataSource, future, connectionGetter, failover, curRunLogPrefix);
    }

    private Connection getConnection(HaDataSource haDataSource, Future<Connection> future, CancellableConnectionGetter connectionGetter, boolean failover, String curRunLogPrefix) throws InterruptedException {
        try {
            return future.get(connectionGettingTimeout, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            logger.error(curRunLogPrefix + "unexpected exception while getting (with timeout) connection to " + haDataSource.desc(failover), e.getCause());
            return null;
        } catch (TimeoutException e) {
            logger.error(curRunLogPrefix + "could not get connection to " + haDataSource.desc(failover) + " within " + connectionGettingTimeout + " second(s); assuming that the data source is unavailable; cancelling connection creation", e);
            connectionGetter.cancel();
            // TODO: jeśli getConnection pobierają co najmniej dwa wątki, to tu powinno być future.cancel(true) - do zastanowienia, czy faktycznie tak by było lepiej
            future.cancel(false);
            return null;
        } catch (InterruptedException e) {
            logger.info(curRunLogPrefix + "interrupted while getting (with timeout) connection to " + haDataSource.desc(failover) + "; interrupting connection creation", e);
            connectionGetter.cancel();
            future.cancel(true);
            throw e;
        } catch (RuntimeException e) {
            logger.error(curRunLogPrefix + "unexpected runtime exception while invoking future.get to get connection to " + haDataSource.desc(failover), e);
            connectionGetter.cancel();
            future.cancel(false);
            return null;
        }
    }

    private void close(HaDataSource haDataSource, String curRunLogPrefix, PreparedStatement preparedStatement, Connection connection, boolean failover) {
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

    private static String nanosToMillis(long l) {
        if (l < Long.MAX_VALUE) {
            return decimalFormat.format(((double) l) / HaDataSource.nanosInMillisecond);
        } else {
            return "<N/A because of exception(s) - assuming infinity>";
        }
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatementExecutionTimeout(int statementExecutionTimeout) {
        this.statementExecutionTimeout = statementExecutionTimeout;
    }

    public void setStatementExecutionTimeoutForcingPeriodMillis(int statementExecutionTimeoutForcingPeriodMillis) {
        this.statementExecutionTimeoutForcingPeriodMillis = statementExecutionTimeoutForcingPeriodMillis;
    }

    public void setConnectionGettingTimeout(int connectionGettingTimeout) {
        this.connectionGettingTimeout = connectionGettingTimeout;
    }
}
