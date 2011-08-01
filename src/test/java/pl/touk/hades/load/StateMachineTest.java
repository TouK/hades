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

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.HashSet;
import static pl.touk.hades.load.LoadLevel.low;
import static pl.touk.hades.load.LoadLevel.medium;
import static pl.touk.hades.load.LoadLevel.high;

/**
 * TODO: description
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class StateMachineTest {

    @Test
    public void shouldMakeTransitions() {
        StateMachine stateMachine = createStateMachine();
        stateMachine.transition(new Load(high, true));
        assertTrue(stateMachine.getCurrentState().isFailoverActive());
        stateMachine.transition(new Load(low, true));
        assertFalse(stateMachine.getCurrentState().isFailoverActive());
        stateMachine.transition(new Load(high, low));
        assertTrue(stateMachine.getCurrentState().isFailoverActive());
        stateMachine.transition(new Load(medium));
        assertTrue(stateMachine.getCurrentState().isFailoverActive());
    }

    private StateMachine createStateMachine() {
        ArrayList<Transition> transitions = new ArrayList<Transition>();
        HashSet<State> mainDbStates = new HashSet<State>();
        mainDbStates.add(new State(false, low));
        mainDbStates.add(new State(false, low, medium));
        mainDbStates.add(new State(false, low, high));
        mainDbStates.add(new State(false, medium, low));
        mainDbStates.add(new State(false, medium));
        mainDbStates.add(new State(false, medium, high));
        mainDbStates.add(new State(false, high, false));
        StateMachine.appendAllPossibleTransitionsBetweenStates(mainDbStates, transitions);
        HashSet<State> failoverDbStates = new HashSet<State>();
        failoverDbStates.add(new State(true, high, low));
        failoverDbStates.add(new State(true, high, medium));
        failoverDbStates.add(new State(true, high, true));
        failoverDbStates.add(new State(true, medium, low));
        failoverDbStates.add(new State(true, medium));
        StateMachine.appendAllPossibleTransitionsBetweenStates(failoverDbStates, transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, high, low), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, high, medium), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, high, true), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low, medium), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low, high), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, medium, high), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, high, false), transitions);
        return new StateMachine(transitions, new State(false, low));
    }
}
