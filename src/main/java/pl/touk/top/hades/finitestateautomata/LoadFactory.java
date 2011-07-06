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
package pl.touk.top.hades.finitestateautomata;

/**
 * A factory that produces an instance of {@link Load} class when given a pair of numeric values: the main database load
 * and the failover database load. Such a numeric pair is transformed into a load (which is descriptive rather than
 * numeric) as follows.
 * Each factory holds two numeric values: lower limit and
 * higher limit (which can be set in {@link #LoadFactory(long, long)}. Numeric values not greater than the lower limit
 * indicate {@link LoadLevel#low low} load level.
 * Numeric values greater than the lower limit and not greater than the higher limit indicate
 * {@link LoadLevel#medium medium} load level.
 * Numeric values greater than the higher limit and less than <code>Long.MAX_VALUE</code>
 * indicate {@link LoadLevel#high high} load level. Numeric values equal to <code>Long.MAX_VALUE</code> indicate
 * {@link LoadLevel#exceptionWhileMeasuring exceptionWileMeasuring} load level.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class LoadFactory {

    private long lowerLimit;
    private long higherLimit;

    /**
     * Constructs a factory with the given higher and lower limits.
     *
     * @param higherLimit higher limit
     * @param lowerLimit lower limit
     */
    public LoadFactory(long higherLimit, long lowerLimit) {
        if (higherLimit < lowerLimit) {
            throw new IllegalArgumentException("higherLimit < lowerLimit");
        }
        if (higherLimit <= 0) {
            throw new IllegalArgumentException("higherLimit <= 0");
        }
        if (lowerLimit <= 0) {
            throw new IllegalArgumentException("lowerLimit <= 0");
        }
        this.higherLimit = higherLimit;
        this.lowerLimit = lowerLimit;
    }

    public Load getLoad(long mainDbLoad, long failoverDbLoad) {
        LoadLevel mainDbLoadLevel = getLoadLevel(mainDbLoad);
        LoadLevel failoverDbLoadLevel = getLoadLevel(failoverDbLoad);
        if (mainDbLoadLevel != failoverDbLoadLevel) {
            return new Load(mainDbLoadLevel, failoverDbLoadLevel);
        } else {
            return new Load(mainDbLoadLevel, mainDbLoad > failoverDbLoad);
        }
    }

    private LoadLevel getLoadLevel(long load) {
        if (load <= lowerLimit) {
            return LoadLevel.low;
        } else if (load <= higherLimit) {
            return LoadLevel.medium;
        } else if (load < Long.MAX_VALUE) {
            return LoadLevel.high;
        } else {
            return LoadLevel.exceptionWhileMeasuring;
        }
    }
}
