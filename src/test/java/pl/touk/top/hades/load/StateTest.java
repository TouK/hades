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
package pl.touk.top.hades.load;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.ArrayList;

import pl.touk.top.hades.load.LoadLevel;
import pl.touk.top.hades.load.State;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class StateTest {

    @Test
    public void shouldBeEqual() {
        // given:
        List<State> states1 = new ArrayList<State>();
        List<State> states2 = new ArrayList<State>();

        // when:
        for (LoadLevel loadLevel: LoadLevel.values()) {
            add(states1, states2, true, loadLevel);
            add(states1, states2, false, loadLevel);
        }

        // then:
        assertEquals(states1, states2);
    }

    @Test
    public void shouldNotBeEqual() {
        // given:
        List<State> states1 = new ArrayList<State>();
        List<State> states2 = new ArrayList<State>();

        // when:
        addSomeNonEqualPairsOfStates(states1, states2);

        // then:
        for (int i = 0; i < states1.size(); i++) {
            assertFalse(states1.get(i).equals(states2.get(i)));
        }
    }

    private void addSomeNonEqualPairsOfStates(List<State> states1, List<State> states2) {
        states1.add(new State(true, LoadLevel.low, true));
        states2.add(new State(true, LoadLevel.low, false));

        states1.add(new State(true, LoadLevel.notMeasuredYet, true));
        states2.add(new State(true, LoadLevel.exceptionWhileMeasuring, true));

        states1.add(new State(true, LoadLevel.low, true));
        states2.add(new State(true, LoadLevel.medium, true));
    }

    private void add(List<State> states1, List<State> states2, boolean failoverActive, LoadLevel loadLevel) {
        states1.add(new State(failoverActive, loadLevel));
        states2.add(new State(failoverActive, loadLevel));
    }
}
