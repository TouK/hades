/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql;

import pl.touk.hades.Hades;
import pl.touk.hades.sql.exception.ConnException;
import pl.touk.hades.sql.timemonitoring.MonitorRunLogPrefix;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;

public class HadesSafeConnectionGetter extends SafeConnectionGetter {

    private final Hades hades;
    private final boolean failover;

    public HadesSafeConnectionGetter(int connTimeoutMillis, Hades hades, boolean failover) {
        super(connTimeoutMillis, hades.desc(failover));
        this.hades = hades;
        this.failover = failover;
    }

    public HadesSafeConnectionGetter(int connTimeoutMillis, Hades hades, boolean failover, ExecutorService executor) {
        super(connTimeoutMillis, hades.desc(failover), executor);
        this.hades = hades;
        this.failover = failover;
    }

    @Override
    protected Connection getConnection(MonitorRunLogPrefix logPrefix) throws ConnException {
        return hades.getConnection(logPrefix, failover);
    }
}

