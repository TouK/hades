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

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.HashSet;
import static pl.touk.hades.load.LoadLevel.low;
import static pl.touk.hades.load.LoadLevel.medium;
import static pl.touk.hades.load.LoadLevel.high;
import pl.touk.hades.load.statemachine.MachineState;
import pl.touk.hades.load.statemachine.Machine;
import pl.touk.hades.load.statemachine.Transition;
import pl.touk.hades.load.HadesLoad;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class MachineTest {

    @Test
    public void shouldMakeTransitions() {
        Machine stateMachine = createStateMachine();
        MachineState state = stateMachine.transition(new MachineState(false, low), new HadesLoad(high, true));
        assertTrue(state.isFailoverActive());
        state = stateMachine.transition(state, new HadesLoad(low, true));
        assertFalse(state.isFailoverActive());
        state = stateMachine.transition(state, new HadesLoad(high, low));
        assertTrue(state.isFailoverActive());
        state = stateMachine.transition(state, new HadesLoad(medium));
        assertTrue(state.isFailoverActive());
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
