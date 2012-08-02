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

import pl.touk.hades.Utils;

import java.util.LinkedList;
import java.io.Serializable;

/**
 * A class that maintains a history (a FIFO list) of last <code>N</code> (<code>N</code> is configurable) times of an sql
 * statement execution. The history can be updated with new execution time in {@link #updateAverage(long)} (this method
 * returns also the new {@link Average} after the update). The way the average is calculated is configured in
 * constructor {@link #SqlTimeHistory(int, boolean, boolean)}.
 * <p>
 * An execution time of <code>Long.MAX_VALUE</code> is treated specialy by this class. It is called
 * <i>infinity</i> and it means that the execution time could not actually be measured (because of an exception
 * for example).
 * <p>
 * This class also uses the notion of <i>recovery</i>. A <i>recovery</i> is a moment in the history when
 * the newest execution time before this moment is finite and the next execution time is <i>infinity</i>.
 * A moment in the history is said to be <i>after recovery</i> when at least one <i>infinity</i> is present
 * in the history before this moment but the newest execution time before this moment is finite.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
class SqlTimeHistory implements Serializable, Cloneable {

    private static final long serialVersionUID = 1059567443788079262L;

    private long total = 0;
    private final int itemsCountIncludedInAverage;

    private long totalFromLastRecovery = 0;
    private int itemsCountFromLastRecovery;

    private final boolean infinitiesNotIncludedInAverageAfterRecovery;
    private final boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery;

    private LinkedList<Long> items;

    private int infinitiesCount = 0;

    @Override
    public SqlTimeHistory clone() {
        try {
            SqlTimeHistory copy = (SqlTimeHistory) super.clone();
            copy.items = new LinkedList<Long>(copy.items);
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs and configures the history.
     *
     * @param itemsCountIncludedInAverage maximal number of elements kept in the history (maximal history size)
     * @param infinitiesNotIncludedInAverageAfterRecovery whether to include <i>infinities</i> in the average after recovery
     * @param recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery whether to include in the average finite values before the last recovery
     */
    public SqlTimeHistory(int itemsCountIncludedInAverage,
                          boolean infinitiesNotIncludedInAverageAfterRecovery,
                          boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery) {
        Utils.assertPositive(itemsCountIncludedInAverage, "itemsCountIncludedInAverage");

        this.itemsCountIncludedInAverage = itemsCountIncludedInAverage;
        this.infinitiesNotIncludedInAverageAfterRecovery = infinitiesNotIncludedInAverageAfterRecovery;
        this.recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery = recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery;
        this.items = new LinkedList<Long>();
    }

    /**
     * Adds the given execution time to the history. If maximal history size is exceeded, removes the
     * last (oldest) executin time from it. Returns the new, updated {@link Average}. If the specified execution time
     * is <i>infinity</i> then the returned average is also infinity, i.e. an average constructed like this:
     * <p>
     * <code>new {@link Average#Average(long, int, long) Average(Long.MAX_VALUE, &lt;current history size&gt;, Long.MAX_VALUE)}</code>).
     *
     * @param executionTime new execution time that should be added to the history
     * @return new, updated average
     */
    public Average updateAverage(long executionTime) {
        removeFirstItem();
        addLastItem(executionTime);
        return getAverage();
    }

    private Average getAverage() {
        if (infinitiesCount == 0) {
            return new Average(total / items.size(), items.size(), items.getLast());
        } else {
            if (itemsCountFromLastRecovery > 0 && infinitiesNotIncludedInAverageAfterRecovery) {
                if (recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery) {
                    return new Average(totalFromLastRecovery / itemsCountFromLastRecovery, itemsCountFromLastRecovery, items.getLast());
                } else {
                    return new Average(total / (items.size()-infinitiesCount), items.size()-infinitiesCount, items.getLast());
                }
            } else {
                return new Average(ExceptionEnum.loadMeasuringException.value(), items.size(), items.getLast());
            }
        }
    }

    private void removeFirstItem() {
        if (items.size() == itemsCountIncludedInAverage) {
            long removedItem = items.removeFirst();
            if (removedItem < ExceptionEnum.minErroneousValue()) {
                total -= removedItem;
                if (infinitiesCount == 0) {
                    totalFromLastRecovery -= removedItem;
                    itemsCountFromLastRecovery--;
                }
            } else {
                infinitiesCount--;
            }
        }
    }

    private void addLastItem(long item) {
        items.addLast(item);
        if (item < ExceptionEnum.minErroneousValue()) {
            total += item;
            totalFromLastRecovery += item;
            itemsCountFromLastRecovery++;
        } else {
            infinitiesCount++;
            totalFromLastRecovery = 0;
            itemsCountFromLastRecovery = 0;
        }
    }

    // equals and hashCode generated with all fields used.
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SqlTimeHistory that = (SqlTimeHistory) o;

        if (infinitiesCount != that.infinitiesCount) return false;
        if (infinitiesNotIncludedInAverageAfterRecovery != that.infinitiesNotIncludedInAverageAfterRecovery)
            return false;
        if (itemsCountFromLastRecovery != that.itemsCountFromLastRecovery) return false;
        if (itemsCountIncludedInAverage != that.itemsCountIncludedInAverage) return false;
        if (recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery != that.recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery)
            return false;
        if (total != that.total) return false;
        if (totalFromLastRecovery != that.totalFromLastRecovery) return false;
        if (items != null ? !items.equals(that.items) : that.items != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (total ^ (total >>> 32));
        result = 31 * result + itemsCountIncludedInAverage;
        result = 31 * result + (int) (totalFromLastRecovery ^ (totalFromLastRecovery >>> 32));
        result = 31 * result + itemsCountFromLastRecovery;
        result = 31 * result + (infinitiesNotIncludedInAverageAfterRecovery ? 1 : 0);
        result = 31 * result + (recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery ? 1 : 0);
        result = 31 * result + (items != null ? items.hashCode() : 0);
        result = 31 * result + infinitiesCount;
        return result;
    }

    @Override
    public String toString() {
        return "SqlTimeHistory{" +
                "total=" + total +
                ", itemsCountIncludedInAverage=" + itemsCountIncludedInAverage +
                ", totalFromLastRecovery=" + totalFromLastRecovery +
                ", itemsCountFromLastRecovery=" + itemsCountFromLastRecovery +
                ", infinitiesNotIncludedInAverageAfterRecovery=" + infinitiesNotIncludedInAverageAfterRecovery +
                ", recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery=" + recoveryErasesHistoryIfInfinitiesNotIncludedInAverageAfterRecovery +
                ", items=" + items +
                ", infinitiesCount=" + infinitiesCount +
                '}';
    }
}
