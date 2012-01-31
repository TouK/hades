/*
 * Copyright (c) 2011 TouK
 * All rights reserved
 */
package pl.touk.hades.load;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.HaDataSource;

/**
 * Implementation of <code>Callable</code> that creates a connection to a given {@link pl.touk.hades.HaDataSource}.
 * The creation
 * process (which is started by invocation of <code>call</code> method by some thread) can be safely cancelled (by
 * invocation of <code>cancel</code> method by some other thread), i.e. it is guaranteed that the connection created in
 * <code>call</code> method will be closed (no matter when <code>cancel</code> happens; it can happen even after the
 * connection was created).
 *
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
class CancellableConnectionGetter implements Callable<Connection> {

    static private final Logger logger = LoggerFactory.getLogger(CancellableConnectionGetter.class);

    private final HaDataSource hades;
    private final boolean failover;
    private final String curRunLogPrefix;

    private boolean ended = false;
    private Connection connection = null;
    private boolean cancelled = false;
    private final Object guard = new Object();

    CancellableConnectionGetter(HaDataSource hades, String curRunLogPrefix, boolean failover) {
        this.hades = hades;
        this.curRunLogPrefix = curRunLogPrefix;
        this.failover = failover;
    }

    /**
     * Returns a connection or null if connection can't be created. Doesn't throw exceptions.
     *
     * @return connection or null if connection can't be created
     */
    public Connection call() {
        if (cancelled()) {
            logger.info(curRunLogPrefix + "cancellation detected before getting connection; aborting");
            return null;
        }
        Connection connection;
        try {
            connection = hades.getConnection(failover, false, null, null, curRunLogPrefix);
        } catch (SQLException e) {
            handleException();
            return null;
        } catch (RuntimeException e) {
            handleException();
            return null;
        }
        return returnConnectionOrNullIfCancelInvoked(connection);
    }

    private boolean cancelled() {
        synchronized (guard) {
            return cancelled;
        }
    }

    private void handleException() {
        synchronized (guard) {
            if (!cancelled) {
                ended = true;
            } else {
                logger.info(curRunLogPrefix + "cancel invoked while connection to " + hades.desc(failover) + " was being created in this thread; however, connection closing is unnecessary because connection creation threw an exception (which was logged earlier)");
            }
        }
    }

    private Connection returnConnectionOrNullIfCancelInvoked(Connection connection) {
        synchronized (guard) {
            if (!cancelled) {
                this.ended = true;
                this.connection = connection;
                return connection;
            } else {
                close(connection, true);
                return null;
            }
        }
    }

    private void close(Connection connection, boolean closeInCallMethod) {
        try {
            logger.info(curRunLogPrefix + (closeInCallMethod ? "connection to " + hades.desc(failover) + " created but cancellation detected; closing connection"
                                                             : "cancel invoked but connection to " + hades.desc(failover) + " already created; closing it"));
            connection.close();
            logger.info(curRunLogPrefix + (closeInCallMethod ? "connection to " + hades.desc(failover) + " closed after cancellation detection"
                                                             : "connection to " + hades.desc(failover) + " closed"));
        } catch (SQLException e) {
            logException(e, closeInCallMethod);
        } catch (RuntimeException e) {
            logException(e, closeInCallMethod);
        }
    }

    private void logException(Exception e, boolean closeInCallMethod) {
        logger.error(curRunLogPrefix + (closeInCallMethod ? "failed to close connection to " + hades.desc(failover) + " after cancellation detection"
                                                          : "failed to close connection to " + hades.desc(failover)), e);
    }

    public void cancel() {
        synchronized (guard) {
            if (!ended) {
                cancelled = true;
                logger.info(curRunLogPrefix + "cancel invoked but connection to " + hades.desc(failover) + " not yet created; if it is created it will be closed");
            } else {
                if (connection != null) {
                    close(connection, false);
                } else {
                    logger.info(curRunLogPrefix + "cancel invoked but creation of connection to " + hades.desc(failover) + " already ended (with exception so connection closing is unnecessary)");
                }
            }
        }
    }
}

