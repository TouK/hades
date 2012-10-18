/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Utils;
import pl.touk.hades.sql.exception.ConnException;
import pl.touk.hades.sql.timemonitoring.MonitorRunLogPrefix;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

public class DsSafeConnectionGetter extends SafeConnectionGetter {

    static private final Logger logger = LoggerFactory.getLogger(DsSafeConnectionGetter.class);

    private final DataSource ds;

    public DsSafeConnectionGetter(int connTimeoutMillis, String dsDesc, DataSource ds) {
        super(connTimeoutMillis, dsDesc);
        this.ds = ds;
    }

    public DsSafeConnectionGetter(int connTimeoutMillis, String dsDesc, DataSource ds, ExecutorService executor) {
        super(connTimeoutMillis, dsDesc, executor);
        this.ds = ds;
    }

    @Override
    protected Connection getConnection(MonitorRunLogPrefix logPrefix) throws ConnException {
        Connection connection;
        String connDesc = "a connection to " + getDsDescription() + " ";
        long start = System.nanoTime();
        try {
            connection = ds.getConnection();
        } catch (Exception e) {
            throw handleException(logPrefix, connDesc, e, start);
        }
        try {
            long timeElapsedNanos = System.nanoTime() - start;
            if (logger.isDebugEnabled()) {
                logger.debug(logPrefix + "successfully got " + connDesc +
                        "in " + Utils.nanosToMillisAsStr(timeElapsedNanos));
            }
            return connection;
        } catch (RuntimeException e) {
            try {
                connection.close();
            } catch (SQLException e1) {
                logger.error(logPrefix + "successfully got" + connDesc + "but unexpected exception occurred (logged " +
                        "below); after that an attempt to close the connection resulted in another exception", e1);
            } finally {
                logger.error(logPrefix + "successfully got" + connDesc + "but unexpected exception occurred", e);
            }
            throw new ConnException(logPrefix, e);
        }
    }

    private ConnException handleException(MonitorRunLogPrefix logPrefix, String connDesc, Exception e, long start) {
        long timeElapsedNanos = System.nanoTime() - start;
        logger.error(logPrefix + "exception while getting " + connDesc +
                "caught in " + Utils.nanosToMillisAsStr(timeElapsedNanos), e);
        return new ConnException(logPrefix, e);
    }
}
