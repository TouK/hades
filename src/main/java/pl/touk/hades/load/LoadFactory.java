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
package pl.touk.hades.load;

/**
 * A factory that produces an instance of {@link Load} class when given a pair of numeric values: the main database load
 * and the failover database load. Such a numeric pair is transformed into a load (which is descriptive rather than
 * numeric) as follows.
 * Each factory holds two numeric values: <code>stmtExecTimeLimitTriggeringFailoverNanos</code> and
 * <code>statementExecutionTimeLimitTriggeringFailbackNanos</code> (which can be set in {@link #LoadFactory(long, long)}),
 * where the first value must not be less than the second value.
 * Numeric values not greater than <code>statementExecutionTimeLimitTriggeringFailbackNanos</code>
 * indicate {@link LoadLevel#low low} load level.
 * Numeric values greater than <code>statementExecutionTimeLimitTriggeringFailbackNanos</code> and not greater than
 * <code>stmtExecTimeLimitTriggeringFailoverNanos</code> indicate {@link LoadLevel#medium medium} load level.
 * Numeric values greater than <code>stmtExecTimeLimitTriggeringFailoverNanos</code> and less than
 * <code>Long.MAX_VALUE</code>
 * indicate {@link LoadLevel#high high} load level. Numeric values equal to <code>Long.MAX_VALUE</code> indicate
 * {@link LoadLevel#exceptionWhileMeasuring exceptionWileMeasuring} load level.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
class LoadFactory {

    private long statementExecutionTimeLimitTriggeringFailbackNanos;
    private long stmtExecTimeLimitTriggeringFailoverNanos;

    /**
     * Constructs a factory with the given higher and lower limits.
     *
     * @param stmtExecTimeLimitTriggeringFailoverNanos higher limit
     * @param statementExecutionTimeLimitTriggeringFailbackNanos lower limit
     */
    public LoadFactory(long stmtExecTimeLimitTriggeringFailoverNanos, long statementExecutionTimeLimitTriggeringFailbackNanos) {
        if (stmtExecTimeLimitTriggeringFailoverNanos < statementExecutionTimeLimitTriggeringFailbackNanos) {
            throw new IllegalArgumentException("stmtExecTimeLimitTriggeringFailover < statementExecutionTimeLimitTriggeringFailback");
        }
        if (stmtExecTimeLimitTriggeringFailoverNanos <= 0) {
            throw new IllegalArgumentException("stmtExecTimeLimitTriggeringFailover <= 0");
        }
        if (statementExecutionTimeLimitTriggeringFailbackNanos <= 0) {
            throw new IllegalArgumentException("statementExecutionTimeLimitTriggeringFailback <= 0");
        }
        this.stmtExecTimeLimitTriggeringFailoverNanos = stmtExecTimeLimitTriggeringFailoverNanos;
        this.statementExecutionTimeLimitTriggeringFailbackNanos = statementExecutionTimeLimitTriggeringFailbackNanos;
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
        if (loadNanos <= statementExecutionTimeLimitTriggeringFailbackNanos) {
            return LoadLevel.low;
        } else if (loadNanos <= stmtExecTimeLimitTriggeringFailoverNanos) {
            return LoadLevel.medium;
        } else if (loadNanos < Long.MAX_VALUE) {
            return LoadLevel.high;
        } else {
            return LoadLevel.exceptionWhileMeasuring;
        }
    }
}
