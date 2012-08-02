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

import org.junit.Test;

import static org.junit.Assert.fail;
import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class SqlTimeHistoryTest {

    @Test
    public void shouldFailIfItemsCountIncludedInAverageIsZero() {
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(true, true, true);
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(true, false, true);
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(true, true, false);
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(true, false, false);
    }

    @Test
    public void shouldFailIfItemsCountIncludedInAverageIsNegative() {
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(false, true, true);
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(false, false, true);
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(false, true, false);
        shouldFailIfItemsCountIncludedInAverageIsNotPositive(false, false, false);
    }

    private void shouldFailIfItemsCountIncludedInAverageIsNotPositive(boolean zeroItemsCount,
                                                                      boolean infinitiesNotIncludedInAverageIfRecovered,
                                                                      boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        try {
            new SqlTimeHistory(zeroItemsCount ? 0 : -1, infinitiesNotIncludedInAverageIfRecovered, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
            fail();
        } catch (IllegalArgumentException e) {
            // ok.
        }
    }

    @Test
    public void shouldReturnLastItemAsAverageWhenItemsCountIncludedInAverageIsOne() {
        shouldReturnLastItemAsAverageWhenItemsCountIncludedInAverageIsOne(true, true);
        shouldReturnLastItemAsAverageWhenItemsCountIncludedInAverageIsOne(false, true);
        shouldReturnLastItemAsAverageWhenItemsCountIncludedInAverageIsOne(true, false);
        shouldReturnLastItemAsAverageWhenItemsCountIncludedInAverageIsOne(false, false);
    }

    private void shouldReturnLastItemAsAverageWhenItemsCountIncludedInAverageIsOne(boolean infinitiesNotIncludedInAverageIfRecovered,
                                                                                   boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        // given:
        SqlTimeHistory calculator;
        long l1 = 10L;
        long l2 = Long.MAX_VALUE;
        long l3 = 20L;
        Average avg1;
        Average avg2;
        Average avg3;

        // when:
        calculator = new SqlTimeHistory(1, infinitiesNotIncludedInAverageIfRecovered, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
        avg1 = calculator.updateAverage(l1);
        avg2 = calculator.updateAverage(l2);
        avg3 = calculator.updateAverage(l3);

        // then:
        assertEquals(l1, avg1.getValue());
        assertEquals(l2, avg2.getValue());
        assertEquals(l3, avg3.getValue());
        assertEquals(1, avg1.getItemsCountIncludedInAverage());
        assertEquals(1, avg2.getItemsCountIncludedInAverage());
        assertEquals(1, avg3.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnInfinityAsLongAsThereAreInfinitiesIncludedInAverage() {
        shouldReturnInfinityAsLongAsThereAreInfinitiesIncludedInAverage(true);
        shouldReturnInfinityAsLongAsThereAreInfinitiesIncludedInAverage(false);
    }

    public void shouldReturnInfinityAsLongAsThereAreInfinitiesIncludedInAverage(boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        // given:
        SqlTimeHistory calculator;
        long l1 = 10L;
        long l2 = 20L;
        Average avg1;
        Average avg2;
        Average avg3;
        Average avg4;
        Average avg5;

        // when:
        calculator = new SqlTimeHistory(2, false, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
        avg1 = calculator.updateAverage(Long.MAX_VALUE);
        avg2 = calculator.updateAverage(Long.MAX_VALUE);
        avg3 = calculator.updateAverage(l1);
        avg4 = calculator.updateAverage(l2);
        avg5 = calculator.updateAverage(Long.MAX_VALUE);

        // then:
        assertEquals(Long.MAX_VALUE, avg1.getValue());
        assertEquals(Long.MAX_VALUE, avg2.getValue());
        assertEquals(Long.MAX_VALUE, avg3.getValue());
        assertEquals((l1+l2)/2, avg4.getValue());
        assertEquals(Long.MAX_VALUE, avg5.getValue());
        assertEquals(1, avg1.getItemsCountIncludedInAverage());
        assertEquals(2, avg2.getItemsCountIncludedInAverage());
        assertEquals(2, avg3.getItemsCountIncludedInAverage());
        assertEquals(2, avg4.getItemsCountIncludedInAverage());
        assertEquals(2, avg5.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnAverageAsLongAsThereIsNoCrash() {
        shouldReturnAverageAsLongAsThereIsNoCrash(true);
        shouldReturnAverageAsLongAsThereIsNoCrash(false);
    }

    private void shouldReturnAverageAsLongAsThereIsNoCrash(boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        // given:
        SqlTimeHistory calculator;
        long l1 = 10L;
        long l2 = 20L;
        long l3 = 30L;
        Average avg1;
        Average avg2;
        Average avg3;

        // when:
        calculator = new SqlTimeHistory(2, true, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
        avg1 = calculator.updateAverage(l1);
        avg2 = calculator.updateAverage(l2);
        avg3 = calculator.updateAverage(l3);

        // then:
        assertEquals(l1, avg1.getValue());
        assertEquals((int) ((l1 + l2) / 2), avg2.getValue());
        assertEquals((int) ((l2 + l3) / 2), avg3.getValue());
        assertEquals(1, avg1.getItemsCountIncludedInAverage());
        assertEquals(2, avg2.getItemsCountIncludedInAverage());
        assertEquals(2, avg3.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnInfinityAfterImmediateCrash() {
        shouldReturnInfinityAfterImmediateCrash(true);
        shouldReturnInfinityAfterImmediateCrash(false);
    }

    private void shouldReturnInfinityAfterImmediateCrash(boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        // given:
        SqlTimeHistory calculator;
        Average avg1;
        Average avg2;
        Average avg3;

        // when:
        calculator = new SqlTimeHistory(2, true, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
        avg1 = calculator.updateAverage(Long.MAX_VALUE);
        avg2 = calculator.updateAverage(Long.MAX_VALUE);
        avg3 = calculator.updateAverage(Long.MAX_VALUE);

        // then:
        assertEquals(Long.MAX_VALUE, avg1.getValue());
        assertEquals(Long.MAX_VALUE, avg2.getValue());
        assertEquals(Long.MAX_VALUE, avg3.getValue());
        assertEquals(1, avg1.getItemsCountIncludedInAverage());
        assertEquals(2, avg2.getItemsCountIncludedInAverage());
        assertEquals(2, avg3.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnInfinityAfterFastCrash() {
        shouldReturnInfinityAfterFastCrash(true);
        shouldReturnInfinityAfterFastCrash(false);
    }

    private void shouldReturnInfinityAfterFastCrash(boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        // given:
        SqlTimeHistory calculator;
        Average avg1;
        Average avg2;

        // when:
        calculator = new SqlTimeHistory(2, true, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
        calculator.updateAverage(10L);
        avg1 = calculator.updateAverage(Long.MAX_VALUE);
        avg2 = calculator.updateAverage(Long.MAX_VALUE);

        // then:
        assertEquals(Long.MAX_VALUE, avg1.getValue());
        assertEquals(Long.MAX_VALUE, avg2.getValue());
        assertEquals(2, avg1.getItemsCountIncludedInAverage());
        assertEquals(2, avg2.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnInfinityAfterCrash() {
        shouldReturnInfinityAfterCrash(true);
        shouldReturnInfinityAfterCrash(false);
    }

    private void shouldReturnInfinityAfterCrash(boolean recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered) {
        // given:
        SqlTimeHistory calculator;
        Average avg1;
        Average avg2;

        // when:
        calculator = new SqlTimeHistory(2, true, recoveryErasesHistoryIfInfinitiesNotIncludedInAverageIfRecovered);
        calculator.updateAverage(20L);
        calculator.updateAverage(10L);
        calculator.updateAverage(30L);
        avg1 = calculator.updateAverage(Long.MAX_VALUE);
        avg2 = calculator.updateAverage(Long.MAX_VALUE);

        // then:
        assertEquals(Long.MAX_VALUE, avg1.getValue());
        assertEquals(Long.MAX_VALUE, avg2.getValue());
        assertEquals(2, avg1.getItemsCountIncludedInAverage());
        assertEquals(2, avg2.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnAverageFromLastRecovery() {
        // given:
        SqlTimeHistory calculator;
        long l1 = 10L;
        long l2 = 20L;
        long l3 = 30L;
        long l4 = 40L;
        Average avg1;
        Average avg2;
        Average avg3;
        Average avg4;
        Average avg5;

        // when:
        calculator = new SqlTimeHistory(2, true, true);
        calculator.updateAverage(10L);
        calculator.updateAverage(20L);
        calculator.updateAverage(30L);
        calculator.updateAverage(Long.MAX_VALUE);
        calculator.updateAverage(Long.MAX_VALUE);
        avg1 = calculator.updateAverage(l1);
        avg2 = calculator.updateAverage(Long.MAX_VALUE);
        avg3 = calculator.updateAverage(l2);
        avg4 = calculator.updateAverage(l3);
        avg5 = calculator.updateAverage(l4);

        // then:
        assertEquals(l1, avg1.getValue());
        assertEquals(Long.MAX_VALUE, avg2.getValue());
        assertEquals(l2, avg3.getValue());
        assertEquals((int)(l2 + l3)/2, avg4.getValue());
        assertEquals((int)(l3 + l4)/2, avg5.getValue());
        assertEquals(1, avg1.getItemsCountIncludedInAverage());
        assertEquals(2, avg2.getItemsCountIncludedInAverage());
        assertEquals(1, avg3.getItemsCountIncludedInAverage());
        assertEquals(2, avg4.getItemsCountIncludedInAverage());
        assertEquals(2, avg5.getItemsCountIncludedInAverage());
    }

    @Test
    public void shouldReturnAverageIgnoringInfinitiesAndIncludingItemsBeforeCrash() {
        // given:
        SqlTimeHistory calculator;
        long l1 = 10L;
        long l2 = 20L;
        long l3 = 30L;
        long l4 = 40L;
        Average avg1;
        Average avg2;
        Average avg3;
        Average avg4;
        Average avg5;

        // when:
        calculator = new SqlTimeHistory(3, true, false);
        calculator.updateAverage(10L);
        calculator.updateAverage(20L);
        calculator.updateAverage(30L);
        calculator.updateAverage(Long.MAX_VALUE);
        calculator.updateAverage(Long.MAX_VALUE);
        avg1 = calculator.updateAverage(l1);
        avg2 = calculator.updateAverage(Long.MAX_VALUE);
        avg3 = calculator.updateAverage(l2);
        avg4 = calculator.updateAverage(l3);
        avg5 = calculator.updateAverage(l4);

        // then:
        assertEquals(l1, avg1.getValue());
        assertEquals(Long.MAX_VALUE, avg2.getValue());
        assertEquals((int)(l1 + l2)/2, avg3.getValue());
        assertEquals((int)(l2 + l3)/2, avg4.getValue());
        assertEquals((int)(l2 + l3 + l4)/3, avg5.getValue());
        assertEquals(1, avg1.getItemsCountIncludedInAverage());
        assertEquals(3, avg2.getItemsCountIncludedInAverage());
        assertEquals(2, avg3.getItemsCountIncludedInAverage());
        assertEquals(2, avg4.getItemsCountIncludedInAverage());
        assertEquals(3, avg5.getItemsCountIncludedInAverage());
    }
}
