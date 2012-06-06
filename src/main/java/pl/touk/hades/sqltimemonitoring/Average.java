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

import java.io.Serializable;

/**
 * A class containing an average of some set of longs, the size of the set and the last value added to the set.
 * <p>
 * Instances of this class are immutable.
 * 
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
class Average implements Serializable {

    private static final long serialVersionUID = -6089625007094055248L;

    private final long value;
    private final int itemsCountIncludedInAverage;
    private final long last;

    public Average(long value, int itemsCountIncludedInAverage, long last) {
        this.value = value;
        this.itemsCountIncludedInAverage = itemsCountIncludedInAverage;
        this.last = last;
    }

    public long getValue() {
        return value;
    }

    public int getItemsCountIncludedInAverage() {
        return itemsCountIncludedInAverage;
    }

    public long getLast() {
        return last;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Average average = (Average) o;

        if (itemsCountIncludedInAverage != average.itemsCountIncludedInAverage) return false;
        if (last != average.last) return false;
        if (value != average.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (value ^ (value >>> 32));
        result = 31 * result + itemsCountIncludedInAverage;
        result = 31 * result + (int) (last ^ (last >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Average{" +
                "value=" + value +
                ", itemsCountIncludedInAverage=" + itemsCountIncludedInAverage +
                ", last=" + last +
                '}';
    }
}
