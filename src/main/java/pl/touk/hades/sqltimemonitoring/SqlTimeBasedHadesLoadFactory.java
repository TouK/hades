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
package pl.touk.hades.sqltimemonitoring;

import pl.touk.hades.load.LoadLevel;
import pl.touk.hades.load.HadesLoad;
import pl.touk.hades.exception.*;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that produces an instance of {@link pl.touk.hades.load.HadesLoad} class when given a pair of numeric values: the main database load
 * and the failover database load. Such a numeric pair is transformed into a load (which is descriptive rather than
 * numeric) as follows.
 * Each factory holds two numeric values: <code>stmtExecTimeLimitTriggeringFailoverNanos</code> and
 * <code>statementExecutionTimeLimitTriggeringFailbackNanos</code> (which can be set in {@link #SqlTimeBasedHadesLoadFactory(long, long)}),
 * where the first value must not be less than the second value.
 * Numeric values not greater than <code>statementExecutionTimeLimitTriggeringFailbackNanos</code>
 * indicate {@link LoadLevel#low low} load level.
 * Numeric values greater than <code>statementExecutionTimeLimitTriggeringFailbackNanos</code> and not greater than
 * <code>stmtExecTimeLimitTriggeringFailoverNanos</code> indicate {@link LoadLevel#medium medium} load level.
 * Numeric values greater than <code>stmtExecTimeLimitTriggeringFailoverNanos</code> and less than
 * <code>Long.MAX_VALUE</code>
 * indicate {@link LoadLevel#high high} load level. Numeric values equal to <code>Long.MAX_VALUE</code> indicate
 * {@link LoadLevel#stmtExecException exceptionWileMeasuring} load level.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
class SqlTimeBasedHadesLoadFactory implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SqlTimeBasedHadesLoadFactory.class);

    private static final long serialVersionUID = 1278912990679817475L;

    private long sqlTimeTriggeringFailbackNanos;
    private long sqlTimeTriggeringFailoverNanos;

    public static enum Error {
        unexpectedException              {public LocalLoadMeasuringException createException(String logPrefix) { return new UnexpectedException             (logPrefix); }},
        connectionGettingException       {public LocalLoadMeasuringException createException(String logPrefix) { return new ConnectionGettingException      (logPrefix); }},
        connectionGettingTimeout         {public LocalLoadMeasuringException createException(String logPrefix) { return new ConnectionGettingTimeout        (logPrefix); }},
        stmtPrepareException             {public LocalLoadMeasuringException createException(String logPrefix) { return new StmtPrepareException            (logPrefix); }},
        stmtQueryTimeoutSettingException {public LocalLoadMeasuringException createException(String logPrefix) { return new StmtQueryTimeoutSettingException(logPrefix); }},
        stmtExecException                {public LocalLoadMeasuringException createException(String logPrefix) { return new StmtExecException               (logPrefix); }},
        stmtExecTimeout                  {public LocalLoadMeasuringException createException(String logPrefix) { return new StmtExecTimeout                 (logPrefix); }};

        Class<? extends LocalLoadMeasuringException> exceptionClass = createException(null).getClass();

        abstract public LocalLoadMeasuringException createException(String logPrefix);

        public long value() {
            return Long.MAX_VALUE - ordinal();
        }

        public static long minErroneousValue() {
            return values()[values().length - 1].value();
        }

        public static String minErroneousValueAsStr() {
            return "Long.MAX_VALUE - " + (values().length - 1);
        }

        public static long valueForException(LocalLoadMeasuringException e) {
            for (Error error: values()) {
                if (error.exceptionClass == e.getClass()) {
                    return error.value();
                }
            }
            throw new IllegalArgumentException("unknown class for LocalLoadMeasuringException: " + e.getClass());
        }

        public static long erroneousValueAsException(long time, String quartzInstanceId, String logPrefix) throws RemoteLoadMeasuringException {
            if (time < minErroneousValue()) {
                return time;
            } else {
                throw new RemoteLoadMeasuringException(quartzInstanceId, values()[(int) (Long.MAX_VALUE - time)].createException(logPrefix));
            }
        }

        public static String erroneousValuesAsStr(long time) {
            if (time < minErroneousValue()) {
                return Long.toString(time);
            } else {
                return values()[(int) (Long.MAX_VALUE - time)].name();
            }
        }
    }

    /**
     * Constructs a factory with the given higher and lower limits.
     *
     * @param sqlTimeTriggeringFailoverNanos higher limit
     * @param sqlTimeTriggeringFailbackNanos lower limit
     */
    public SqlTimeBasedHadesLoadFactory(long sqlTimeTriggeringFailoverNanos, long sqlTimeTriggeringFailbackNanos) {
        if (sqlTimeTriggeringFailoverNanos < sqlTimeTriggeringFailbackNanos) {
            throw new IllegalArgumentException("stmtExecTimeLimitTriggeringFailover < statementExecutionTimeLimitTriggeringFailback");
        }
        if (sqlTimeTriggeringFailoverNanos <= 0) {
            throw new IllegalArgumentException("stmtExecTimeLimitTriggeringFailover <= 0");
        }
        if (sqlTimeTriggeringFailbackNanos <= 0) {
            throw new IllegalArgumentException("statementExecutionTimeLimitTriggeringFailback <= 0");
        }
        if (sqlTimeTriggeringFailoverNanos >= Error.minErroneousValue()) {
            throw new IllegalArgumentException("sqlTimeTriggeringFailoverNanos >= " + Error.minErroneousValueAsStr());
        }
        this.sqlTimeTriggeringFailoverNanos = sqlTimeTriggeringFailoverNanos;
        this.sqlTimeTriggeringFailbackNanos = sqlTimeTriggeringFailbackNanos;
    }

    public HadesLoad getLoad(long mainDbLoadNanos, long failoverDbLoadNanos) {
        LoadLevel mainDbLoadLevel = getLoadLevel(mainDbLoadNanos);
        LoadLevel failoverDbLoadLevel = getLoadLevel(failoverDbLoadNanos);
        if (mainDbLoadLevel != failoverDbLoadLevel) {
            return new HadesLoad(mainDbLoadLevel, failoverDbLoadLevel);
        } else {
            return new HadesLoad(mainDbLoadLevel, mainDbLoadNanos > failoverDbLoadNanos);
        }
    }

    private LoadLevel getLoadLevel(long loadNanos) {
        if (loadNanos < 0) {
            throw new IllegalArgumentException("loadNanos must not be less than zero: " + loadNanos);
        }

        if (loadNanos <= sqlTimeTriggeringFailbackNanos) {
            return LoadLevel.low;
        } else if (loadNanos <= sqlTimeTriggeringFailoverNanos) {
            return LoadLevel.medium;
        } else if (loadNanos < Error.minErroneousValue()) {
            return LoadLevel.high;
        } else {
            return LoadLevel.exceptionWhileMeasuring;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SqlTimeBasedHadesLoadFactory that = (SqlTimeBasedHadesLoadFactory) o;

        if (sqlTimeTriggeringFailbackNanos != that.sqlTimeTriggeringFailbackNanos) return false;
        if (sqlTimeTriggeringFailoverNanos != that.sqlTimeTriggeringFailoverNanos) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (sqlTimeTriggeringFailbackNanos ^ (sqlTimeTriggeringFailbackNanos >>> 32));
        result = 31 * result + (int) (sqlTimeTriggeringFailoverNanos ^ (sqlTimeTriggeringFailoverNanos >>> 32));
        return result;
    }
}
