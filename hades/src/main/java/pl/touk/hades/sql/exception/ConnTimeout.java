/*
 * Copyright (c) 2012 TouK
 * All rights reserved
 */
package pl.touk.hades.sql.exception;

import pl.touk.hades.exception.LoadMeasuringException;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class ConnTimeout extends LoadMeasuringException {
    public ConnTimeout(String curRunLogPrefix) {
        super(curRunLogPrefix);
    }
}
