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
package pl.touk.hades.load.statemachine;

import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static pl.touk.hades.load.LoadLevel.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import pl.touk.hades.load.Load;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class MachineTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullFromState() {
        Machine.createStateMachine().transition(null, new Load(low, low));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullLoad() {
        Machine.createStateMachine().transition(Machine.initialState, null);
    }

    @Test
    public void shouldKeepFailoverActiveAsLongAsMainLoadIsAtLeastMedium() {
        ArrayList<Load> atLeastMediumMainLoads = new ArrayList<Load>();
        atLeastMediumMainLoads.addAll(Arrays.asList(
                new Load(medium, low),
                new Load(medium),
                new Load(medium, true),
                new Load(medium, false),
                new Load(medium, medium),
                new Load(high, low),
                new Load(high, medium),
                new Load(exceptionWhileMeasuring, medium)));
        assertFailoverStillActiveOrAssertFailback(true, atLeastMediumMainLoads);
    }

    @Test
    public void shouldFailbackWhenMainLoadTurnsFromMediumToLow() {
        ArrayList<Load> lowMainLoads = new ArrayList<Load>();
        lowMainLoads.addAll(Arrays.asList(
                new Load(low),
                new Load(low, true),
                new Load(low, false),
                new Load(low, low),
                new Load(low, medium),
                new Load(low, high),
                new Load(low, exceptionWhileMeasuring)));
        assertFailoverStillActiveOrAssertFailback(false, lowMainLoads);
    }

    @Test
    public void shouldFailbackWhenFailoverLoadTurnsFromMediumToHigh() {
        ArrayList<Load> lowMainLoads = new ArrayList<Load>();
        lowMainLoads.addAll(Arrays.asList(
                new Load(low, high),
                new Load(low, exceptionWhileMeasuring),
                new Load(medium, high),
                new Load(medium, exceptionWhileMeasuring),
                new Load(high, false)));
        assertFailoverStillActiveOrAssertFailback(false, lowMainLoads);
    }

    private void assertFailoverStillActiveOrAssertFailback(boolean failover, List<Load> loads) {
        Machine m = Machine.createStateMachine();
        for (Load l: loads) {
            assertEquals("" + l, failover, m.transition(new MachineState(true, new Load(medium)),         l).isFailoverActive());
            assertEquals("" + l, failover, m.transition(new MachineState(true, new Load(medium, true)),   l).isFailoverActive());
            assertEquals("" + l, failover, m.transition(new MachineState(true, new Load(medium, false)),  l).isFailoverActive());
            assertEquals("" + l, failover, m.transition(new MachineState(true, new Load(medium, medium)), l).isFailoverActive());
        }
    }

    @Test
    public void shouldMakeTransitions() {
        Machine stateMachine = createStateMachine();
        MachineState state = stateMachine.transition(new MachineState(false, low), new Load(high, true));
        assertTrue(state.isFailoverActive());
        state = stateMachine.transition(state, new Load(low, true));
        assertFalse(state.isFailoverActive());
        state = stateMachine.transition(state, new Load(high, low));
        assertTrue(state.isFailoverActive());
        state = stateMachine.transition(state, new Load(medium));
        assertTrue(state.isFailoverActive());
    }

    @Test
    public void shouldNotAllowTransitionForBothLevelsHighWithoutSpecifyingWhichOneIsHigher() throws Exception {
        try {
            Machine.createStateMachine().transition(new MachineState(false, low), new Load(high));
            fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("for Load{main=high,failover=high} not found"));
        }
    }

    @Test
    public void shouldNotAllowTransitionFromLowToNotMeasuredYet() {
        shouldNotAllowTransitionToNotMeasuredYet(new Load(low));
        shouldNotAllowTransitionToNotMeasuredYet(new Load(medium));
        shouldNotAllowTransitionToNotMeasuredYet(new Load(exceptionWhileMeasuring));
        shouldNotAllowTransitionToNotMeasuredYet(new Load(exceptionWhileMeasuring, true));
        shouldNotAllowTransitionToNotMeasuredYet(new Load(exceptionWhileMeasuring, false));
    }

    @Test
    public void shouldAllowTransitionFromNotMeasuredYetToNotMeasuredYet() {
        assertFalse(Machine.createStateMachine().transition(new MachineState(false, notMeasuredYet, notMeasuredYet),
                new Load(notMeasuredYet, notMeasuredYet)).isFailoverActive());
    }

    private void shouldNotAllowTransitionToNotMeasuredYet(Load l) {
        Machine stateMachine = Machine.createStateMachine();
        MachineState state = stateMachine.transition(Machine.initialState, l);
        try {
            stateMachine.transition(state, new Load(notMeasuredYet, notMeasuredYet));
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("return to notMeasuredYet"));
        }
    }

    private Machine createStateMachine() {
        ArrayList<Transition> transitions = new ArrayList<Transition>();
        HashSet<MachineState> mainDbStates = new HashSet<MachineState>();
        mainDbStates.add(new MachineState(false, low));
        mainDbStates.add(new MachineState(false, low, medium));
        mainDbStates.add(new MachineState(false, low, high));
        mainDbStates.add(new MachineState(false, medium, low));
        mainDbStates.add(new MachineState(false, medium));
        mainDbStates.add(new MachineState(false, medium, high));
        mainDbStates.add(new MachineState(false, high, false));
        Machine.appendAllPossibleTransitionsBetweenStates(mainDbStates, transitions);
        HashSet<MachineState> failoverDbStates = new HashSet<MachineState>();
        failoverDbStates.add(new MachineState(true, high, low));
        failoverDbStates.add(new MachineState(true, high, medium));
        failoverDbStates.add(new MachineState(true, high, true));
        failoverDbStates.add(new MachineState(true, medium, low));
        failoverDbStates.add(new MachineState(true, medium));
        Machine.appendAllPossibleTransitionsBetweenStates(failoverDbStates, transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, high, low), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, high, medium), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, high, true), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low, medium), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low, high), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, medium, high), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, high, false), transitions);
        return new Machine(transitions, new MachineState(false, low));
    }
}
