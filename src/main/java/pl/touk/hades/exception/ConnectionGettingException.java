/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.exception;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class ConnectionGettingException extends LocalLoadMeasuringException {
    public ConnectionGettingException(String logPrefix, Exception e) {
        super(logPrefix, e);
    }

    public ConnectionGettingException(String logPrefix) {
        super(logPrefix);
    }
}
