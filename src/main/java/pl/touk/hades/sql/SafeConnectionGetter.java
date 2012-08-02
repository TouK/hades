/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Utils;
import pl.touk.hades.exception.UnexpectedException;
import pl.touk.hades.sql.exception.ConnException;
import pl.touk.hades.sql.exception.ConnTimeout;

import static pl.touk.hades.Utils.indent;

/**
 * Implementation of <code>Callable</code> that creates a connection to a given {@link pl.touk.hades.Hades}.
 * The creation
 * process (which is started by invocation of <code>call</code> method by some thread) can be safely cancelled (by
 * invocation of <code>cancel</code> method by some other thread), i.e. it is guaranteed that the connection created in
 * <code>call</code> method will be closed (no matter when <code>cancel</code> happens; it can happen even after the
 * connection was created).
 *
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
abstract public class SafeConnectionGetter {

    static private final Logger logger = LoggerFactory.getLogger(SafeConnectionGetter.class);

    private final String dsDescription;
    private final int connTimeoutMillis;

    private boolean ended = false;
    private Connection connection = null;
    private boolean cancelled = false;
    private final Object guard = new Object();

    private final ExecutorService externalExecutor;

    SafeConnectionGetter(int connTimeoutMillis, String dsDescription) {
        this(connTimeoutMillis, dsDescription, null);
    }

    SafeConnectionGetter(int connTimeoutMillis, String dsDescription, ExecutorService externalExecutor) {
        Utils.assertPositive(connTimeoutMillis, "connTimeoutMillis");

        this.dsDescription = dsDescription;
        this.connTimeoutMillis = connTimeoutMillis;
        this.externalExecutor = externalExecutor;
    }

    public Connection getConnectionWithTimeout(String logPrefix) throws InterruptedException, UnexpectedException, ConnException, ConnTimeout {
        logger.debug(logPrefix + "getConnectionWithTimeout");
        final String indentedLogPrefix = indent(logPrefix);

        ExecutorService executor = null;
        Connection c = null;
        try {
            if (externalExecutor != null) {
                executor = externalExecutor;
            } else {
                executor = Executors.newSingleThreadExecutor();
            }
            Future<Connection> future = executor.submit(new Callable<Connection>() {
                public Connection call() throws Exception {
                    return safelyGetConnection(indentedLogPrefix);
                }
            });
            return getConnection(indentedLogPrefix, future);
        } catch (RejectedExecutionException e) {
            logger.error(indentedLogPrefix + "unexpected RejectedExecutionException while trying to get connection to " + dsDescription, e);
            throw new UnexpectedException(indentedLogPrefix, e);
        } finally {
            if (externalExecutor == null && executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private Connection getConnection(String logPrefix, Future<Connection> future) throws InterruptedException, ConnException, UnexpectedException, ConnTimeout {
        try {
            return future.get(connTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            return handleConnException(logPrefix, e);
        } catch (TimeoutException e) {
            cancel(logPrefix, future);
            logger.error(logPrefix + "could not get connection to " + dsDescription + " within " + connTimeoutMillis + " millisecond(s); assuming that the data source is unavailable; cancelling connection creation", e);
            throw new ConnTimeout(logPrefix);
        } catch (InterruptedException e) {
            cancel(logPrefix, future);
            logger.info(logPrefix + "interrupted while getting (with timeout) connection to " + dsDescription + "; cancelling connection creation", e);
            throw e;
        } catch (RuntimeException e) {
            cancel(logPrefix, future);
            logger.error(logPrefix + "could not get connection to " + dsDescription + "; assuming that the data source is unavailable; cancelling connection creation", e);
            throw new UnexpectedException(logPrefix, e);
        }
    }

    private void cancel(String logPrefix, Future<Connection> future) {
        cancel(logPrefix);
        future.cancel(true);
    }

    /**
     * Returns a connection.
     *
     * @return connection
     * @throws pl.touk.hades.sql.exception.ConnException if an exception occured
     */
    private Connection safelyGetConnection(String logPrefix) throws ConnException {
        if (cancelled()) {
            logger.info(logPrefix + "cancellation detected before getting connection to " + dsDescription + "; aborting");
            return null;
        }
        Connection connection;
        try {
            connection = getConnection(logPrefix);
        } catch (ConnException e) {
            handleException(logPrefix);
            throw e;
        }
        return returnConnectionOrNullIfCancelInvoked(logPrefix, connection);
    }

    abstract protected Connection getConnection(String logPrefix) throws ConnException;

    private Connection handleConnException(String logPrefix, ExecutionException t) throws ConnException, UnexpectedException {
        if (t.getCause() instanceof ConnException) {
            throw (ConnException) t.getCause();
        } else {
            String s = logPrefix + "unexpected cause of ExecutionException while getting connection to " + getDsDescription();
            logger.error(s, t);
            throw new UnexpectedException(logPrefix, t.getCause());
        }
    }

    private boolean cancelled() {
        synchronized (guard) {
            return cancelled;
        }
    }

    private void handleException(String logPrefix) {
        synchronized (guard) {
            if (!cancelled) {
                ended = true;
            } else {
                logger.info(logPrefix + "cancel invoked while connection to " + dsDescription + " was being created in this thread; however, connection closing is unnecessary because connection creation threw an exception (which was logged earlier)");
            }
        }
    }

    private Connection returnConnectionOrNullIfCancelInvoked(String logPrefix, Connection connection) {
        synchronized (guard) {
            if (!cancelled) {
                this.ended = true;
                this.connection = connection;
                return connection;
            } else {
                close(logPrefix, connection, true);
                return null;
            }
        }
    }

    private void close(String logPrefix, Connection connection, boolean closeInCallMethod) {
        try {
            logger.info(logPrefix + (closeInCallMethod ? "connection to " + dsDescription + " created but cancellation detected; closing connection"
                                                             : "cancel invoked but connection to " + dsDescription + " already created; closing it"));
            connection.close();
            logger.info(logPrefix + (closeInCallMethod ? "connection to " + dsDescription + " closed after cancellation detection"
                                                             : "connection to " + dsDescription + " closed"));
        } catch (SQLException e) {
            logException(logPrefix, e, closeInCallMethod);
        } catch (RuntimeException e) {
            logException(logPrefix, e, closeInCallMethod);
        }
    }

    private void logException(String logPrefix, Exception e, boolean closeInCallMethod) {
        logger.error(logPrefix + (closeInCallMethod ? "failed to close connection to " + dsDescription + " after cancellation detection"
                                                          : "failed to close connection to " + dsDescription), e);
    }

    private void cancel(String logPrefix) {
        synchronized (guard) {
            if (!ended) {
                cancelled = true;
                logger.info(logPrefix + "cancel invoked but connection to " + dsDescription + " not yet created; if it is created it will be closed");
            } else {
                if (connection != null) {
                    close(logPrefix, connection, false);
                } else {
                    logger.info(logPrefix + "cancel invoked but creation of connection to " + dsDescription + " already ended (with exception so connection closing is unnecessary)");
                }
            }
        }
    }

    protected String getDsDescription() {
        return dsDescription;
    }
}

