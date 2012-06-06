/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.exception;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class ConnectionGettingTimeout extends LocalLoadMeasuringException {
    public ConnectionGettingTimeout(String curRunLogPrefix) {
        super(curRunLogPrefix);
    }
}
