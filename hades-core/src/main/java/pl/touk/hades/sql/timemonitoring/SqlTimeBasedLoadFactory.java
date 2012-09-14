/*
 * Copyright 2011 TouK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.load.Load;
import pl.touk.hades.load.LoadLevel;

import java.io.Serializable;

/**
 * A factory that produces an instance of {@link pl.touk.hades.load.Load} class when given a pair of numeric values: the main database load
 * and the failover database load. Such a numeric pair is transformed into a load (which is descriptive rather than
 * numeric) as follows.
 * Each factory holds two numeric values: <code>failbackThresholdNanos</code> and
 * <code>statementExecutionTimeLimitTriggeringFailbackNanos</code> (which can be set in {@link #SqlTimeBasedLoadFactory(long, long)}),
 * where the first value must not be less than the second value.
 * Numeric values not greater than <code>statementExecutionTimeLimitTriggeringFailbackNanos</code>
 * indicate {@link LoadLevel#low low} load level.
 * Numeric values greater than <code>statementExecutionTimeLimitTriggeringFailbackNanos</code> and not greater than
 * <code>failbackThresholdNanos</code> indicate {@link LoadLevel#medium medium} load level.
 * Numeric values greater than <code>failbackThresholdNanos</code> and less than
 * <code>Long.MAX_VALUE</code>
 * indicate {@link LoadLevel#high high} load level. Numeric values greater than or equal to
 * {@link ExceptionEnum#minErroneousValue() ExceptionEnum.minErroneousValue()}
 * indicate {@link LoadLevel#exceptionWhileMeasuring exceptionWileMeasuring} load level.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class SqlTimeBasedLoadFactory implements Serializable {

    private static final long serialVersionUID = 1278912990679817475L;

    private final long failoverThresholdNanos;
    private final long failbackThresholdNanos;

    /**
     * Constructs a factory with the given higher and lower limits.
     *
     * @param failoverThresholdNanos higher limit
     * @param failbackThresholdNanos lower limit
     */
    public SqlTimeBasedLoadFactory(long failoverThresholdNanos, long failbackThresholdNanos) {
        if (failoverThresholdNanos <= 0) {
            throw new IllegalArgumentException("failoverThresholdNanos <= 0");
        }
        if (failbackThresholdNanos <= 0) {
            throw new IllegalArgumentException("failbackThresholdNanos <= 0");
        }
        if (failoverThresholdNanos >= ExceptionEnum.minErroneousValue()) {
            throw new IllegalArgumentException("failoverThresholdNanos >= minimum erroneous value (" + ExceptionEnum.minErroneousValueAsStr() + ")");
        }
        if (failbackThresholdNanos >= ExceptionEnum.minErroneousValue()) {
            throw new IllegalArgumentException("failbackThresholdNanos >= minimum erroneous value (" + ExceptionEnum.minErroneousValueAsStr() + ")");
        }
        if (failoverThresholdNanos < failbackThresholdNanos) {
            throw new IllegalArgumentException("failoverThresholdNanos < failbackThresholdNanos");
        }
        this.failoverThresholdNanos = failoverThresholdNanos;
        this.failbackThresholdNanos = failbackThresholdNanos;
    }

    public Load getLoad(long mainDbLoadNanos, long failoverDbLoadNanos) {
        LoadLevel mainDbLoadLevel = getLoadLevel(mainDbLoadNanos);
        LoadLevel failoverDbLoadLevel = getLoadLevel(failoverDbLoadNanos);
        if (mainDbLoadLevel != failoverDbLoadLevel) {
            return new Load(mainDbLoadLevel, failoverDbLoadLevel);
        } else {
            return new Load(mainDbLoadLevel, mainDbLoadNanos > failoverDbLoadNanos);
        }
    }

    private LoadLevel getLoadLevel(long loadNanos) {
        if (loadNanos < 0) {
            throw new IllegalArgumentException("loadNanos must not be less than zero: " + loadNanos);
        }

        if (loadNanos <= failbackThresholdNanos) {
            return LoadLevel.low;
        } else if (loadNanos <= failoverThresholdNanos) {
            return LoadLevel.medium;
        } else if (loadNanos < ExceptionEnum.minErroneousValue()) {
            return LoadLevel.high;
        } else {
            return LoadLevel.exceptionWhileMeasuring;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SqlTimeBasedLoadFactory that = (SqlTimeBasedLoadFactory) o;

        if (failbackThresholdNanos != that.failbackThresholdNanos) return false;
        if (failoverThresholdNanos != that.failoverThresholdNanos) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (failbackThresholdNanos ^ (failbackThresholdNanos >>> 32));
        result = 31 * result + (int) (failoverThresholdNanos ^ (failoverThresholdNanos >>> 32));
        return result;
    }

    public long getFailbackThresholdNanos() {
        return failbackThresholdNanos;
    }

    public long getFailoverThresholdNanos() {
        return failoverThresholdNanos;
    }

    @Override
    public String toString() {
        return "SqlTimeBasedLoadFactory{" +
                "failoverThresholdNanos=" + failoverThresholdNanos +
                ", failbackThresholdNanos=" + failbackThresholdNanos +
                '}';
    }
}
