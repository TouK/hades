package pl.touk.hades.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Utils;
import pl.touk.hades.exception.UnexpectedException;
import pl.touk.hades.sql.exception.SqlExecException;
import pl.touk.hades.sql.exception.SqlExecTimeout;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.*;

public class SafeSqlExecutor {

    private static final Logger logger = LoggerFactory.getLogger(SafeSqlExecutor.class);

    private final int sqlExecTimeout;
    private final int sqlExecTimeoutForcingPeriodMillis;
    private final String dsDesc;

    private int updatedCount;

    private final ExecutorService externalExecutor;

    public SafeSqlExecutor(int sqlExecTimeout, int sqlExecTimeoutForcingPeriodMillis, String dsDesc) {
        this(sqlExecTimeout, sqlExecTimeoutForcingPeriodMillis, dsDesc, null);
    }

    public SafeSqlExecutor(int sqlExecTimeout, int sqlExecTimeoutForcingPeriodMillis, String dsDesc, ExecutorService externalExecutor) {
        Utils.assertNonNegative(sqlExecTimeout, "sqlExecTimeout");
        Utils.assertNonNegative(sqlExecTimeoutForcingPeriodMillis, "sqlExecTimeoutForcingPeriodMillis");

        this.sqlExecTimeout = sqlExecTimeout;
        this.sqlExecTimeoutForcingPeriodMillis = sqlExecTimeoutForcingPeriodMillis;
        this.dsDesc = dsDesc;
        this.externalExecutor = externalExecutor;
    }

    public long execute(String logPrefix, PreparedStatement preparedStatement, boolean update, String sql) throws InterruptedException, SqlExecException, UnexpectedException, SqlExecTimeout {
        long timeNanos;
        if (sqlExecTimeout > 0) {
            timeNanos = executeWithTimeout(logPrefix, preparedStatement, update, sql);
        } else {
            timeNanos = executeWithoutTimeout(logPrefix, preparedStatement, update, sql);
        }
        logger.debug(logPrefix + "executed in " + timeNanos + " ns: " + sql);
        return timeNanos;
    }

    private long executeWithoutTimeout(String logPrefix, PreparedStatement preparedStatement, boolean update, String sql) throws SqlExecException, UnexpectedException {
        long start = System.nanoTime();
        try {
            if (update) {
                updatedCount = preparedStatement.executeUpdate();
            } else {
                preparedStatement.execute();
            }
            return System.nanoTime() - start;
        } catch (SQLException e) {
            logger.error(sql, e);
            throw new SqlExecException(logPrefix, handleException(logPrefix, start, e));
        } catch (RuntimeException e) {
            throw new UnexpectedException(logPrefix, handleException(logPrefix, start, e));
        }
    }

    private <T extends Exception> T handleException(String curRunLogPrefix, long start, T e) {
        long duration = System.nanoTime() - start;
        logger.error(curRunLogPrefix + "exception while executing statement on " + dsDesc + " caught in " + Utils.nanosToMillisAsStr(duration) + " ms", e);
        return e;
    }

    private long executeWithTimeout(final String logPrefix, final PreparedStatement preparedStatement, final boolean update, final String sql) throws InterruptedException, UnexpectedException, SqlExecException, SqlExecTimeout {
        ExecutorService executor = null;
        try {
            if (externalExecutor != null) {
                executor = externalExecutor;
            } else {
                executor = Executors.newSingleThreadExecutor();
            }
            Future<Long> future = executor.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    return executeWithoutTimeout(logPrefix, preparedStatement, update, sql);
                }
            });
            return execute(logPrefix, future);
        } catch (RejectedExecutionException e) {
            logger.error(logPrefix + "unexpected RejectedExecutionException while trying to execute statement on " + dsDesc, e);
            throw new UnexpectedException(logPrefix, e);
        } finally {
            if (externalExecutor == null && executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private long execute(String logPrefix, Future<Long> future) throws InterruptedException, SqlExecException, UnexpectedException, SqlExecTimeout {
        try {
            return future.get(forcedStatementExecutionTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            return handleExceptionsOfExecuteWithoutTimeoutMethod(logPrefix, e);
        } catch (TimeoutException e) {
            logger.error(logPrefix + "could not execute statement on " + dsDesc + " within " + forcedStatementExecutionTimeoutMillis() + " millisecond(s)");
            throw new SqlExecTimeout(logPrefix);
        } catch (InterruptedException e) {
            logger.info(logPrefix + "interrupted while invoking future.get to execute statement on " + dsDesc + "; interrupting statement execution", e);
            throw e;
        } catch (RuntimeException e) {
            logger.error(logPrefix + "unexpected exception while executing statement on " + dsDesc, e);
            throw new UnexpectedException(logPrefix, e);
        } finally {
            future.cancel(true);
        }
    }

    private int forcedStatementExecutionTimeoutMillis() {
        return sqlExecTimeout * 1000 + sqlExecTimeoutForcingPeriodMillis;
    }

    private long handleExceptionsOfExecuteWithoutTimeoutMethod(String logPrefix, ExecutionException t) throws SqlExecException, UnexpectedException {
        if (t.getCause() instanceof SqlExecException) {
            throw (SqlExecException) t.getCause();
        } else {
            String s = logPrefix + "unexpected cause of ExecutionException while executing sql on " + dsDesc;
            logger.error(s, t);
            throw new UnexpectedException(logPrefix, t.getCause());
        }
    }

    public int getUpdatedCount() {
        return updatedCount;
    }
}
