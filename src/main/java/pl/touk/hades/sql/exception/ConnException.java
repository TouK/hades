/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql.exception;

import pl.touk.hades.exception.LoadMeasuringException;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class ConnException extends LoadMeasuringException {
    public ConnException(String logPrefix, Exception e) {
        super(logPrefix, e);
    }

    public ConnException(String logPrefix) {
        super(logPrefix);
    }
}
