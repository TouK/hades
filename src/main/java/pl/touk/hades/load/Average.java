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
 * A class containing an average of some set of longs, the size of the set and the last value added to the set.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
class Average {

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
}
