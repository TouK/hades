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

import static pl.touk.hades.load.LoadLevel.*;
import static pl.touk.hades.load.LoadLevel.exceptionWhileMeasuring;
import static pl.touk.hades.load.LoadLevel.notMeasuredYet;

import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;
import pl.touk.hades.load.LoadLevel;

import java.util.*;

/**
 * A deterministic finite-state machine that can be used to control failover activation.
 * States are of type {@link MachineState}. The current state of the machine holds the current state of failover activation.
 * When an input symbol <code>L</code> of type {@link pl.touk.hades.load.Load}
 * is provided a {@link pl.touk.hades.load.statemachine.Transition} <code>T</code> is made such that <code>T.sourceState</code> is equal to the current state,
 * <code>T.destinationState.load</code> is equal to <code>L</code> and <code>T</code> is among possible transitions
 * for this machine (possible transition are specified in the constructor).
 * <p>
 * A transition <code>S1->S2</code> indicates a <code>failover</code> if
 * <code>S1.failoverActive == false</code> and <code>S2.failoverActive == true</code>.
 * A transition <code>S1->S2</code> indicates a <code>failback</code> if
 * <code>S1.failoverActive == true</code> and <code>S2.failoverActive == false</code>.
 *
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class Machine {

    public static final MachineState initialState = new MachineState(false, notMeasuredYet);

    private final Map<MachineState, Map<Load, Boolean>> transitions = new HashMap<MachineState, Map<Load, Boolean>>();

    public static Machine createStateMachine() {
        ArrayList<Transition> transitions = new ArrayList<Transition>();

        // All states when failover is inactive:
        HashSet<MachineState> mainDbStates = new HashSet<MachineState>();
        mainDbStates.add(new MachineState(false, low));
        mainDbStates.add(new MachineState(false, low, medium));
        mainDbStates.add(new MachineState(false, low, high));
        mainDbStates.add(new MachineState(false, low, exceptionWhileMeasuring));
        mainDbStates.add(new MachineState(false, medium, low));
        mainDbStates.add(new MachineState(false, medium));
        mainDbStates.add(new MachineState(false, medium, high));
        mainDbStates.add(new MachineState(false, medium, exceptionWhileMeasuring));
        mainDbStates.add(new MachineState(false, high, false));
        mainDbStates.add(new MachineState(false, high, exceptionWhileMeasuring));
        mainDbStates.add(new MachineState(false, exceptionWhileMeasuring, exceptionWhileMeasuring));
        mainDbStates.add(new MachineState(false, notMeasuredYet));
        // All possible transitions between above states keep failover inactive:
        Machine.appendAllPossibleTransitionsBetweenStates(mainDbStates, transitions);

        // All states when failover is active:
        HashSet<MachineState> failoverDbStates = new HashSet<MachineState>();
        failoverDbStates.add(new MachineState(true, exceptionWhileMeasuring, low));
        failoverDbStates.add(new MachineState(true, exceptionWhileMeasuring, medium));
        failoverDbStates.add(new MachineState(true, exceptionWhileMeasuring, high));
        failoverDbStates.add(new MachineState(true, high, low));
        failoverDbStates.add(new MachineState(true, high, medium));
        failoverDbStates.add(new MachineState(true, high, true));
        failoverDbStates.add(new MachineState(true, medium, low));
        failoverDbStates.add(new MachineState(true, medium));
        // All possible transitions between above states keep failover active:
        Machine.appendAllPossibleTransitionsBetweenStates(failoverDbStates, transitions);

        // Activate failover when main database load becomes high and the failover database works and its load is smaller:
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, high, low), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, high, medium), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, high, true), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, exceptionWhileMeasuring, low), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, exceptionWhileMeasuring, medium), transitions);
        Machine.appendAllPossibleTransitionsFromStates(mainDbStates, new MachineState(true, exceptionWhileMeasuring, high), transitions);

        // Activate failback when main database load returns to low or becomes smaller than the high failover database load:
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low, medium), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low, high), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, medium, high), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, high, false), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, low, exceptionWhileMeasuring), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, medium, exceptionWhileMeasuring), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, high, exceptionWhileMeasuring), transitions);
        Machine.appendAllPossibleTransitionsFromStates(failoverDbStates, new MachineState(false, exceptionWhileMeasuring, exceptionWhileMeasuring), transitions);

        return new Machine(transitions, initialState);
    }

    /**
     * For the given set of states creates all possible transitions and appends them to the given transition list.
     *
     * @param states set of states for which all possible transitions should be created and added to the given transition list
     * @param transitionList list of transitions which should be extended with all possible transitions between given states
     */
    public static void appendAllPossibleTransitionsBetweenStates(Set<MachineState> states, List<Transition> transitionList) {
        for (MachineState state1 : states) {
            for (MachineState state2 : states) {
                transitionList.add(new Transition(state1, state2));
            }
        }
    }

    /**
     * For the given set of source states and the given destination state creates all possible transitions and appends them
     * to the given list of transitions.
     *
     * @param sourceStateSet set of source states for transitions that should be created
     * @param destinationState destination state for transitions that should be created
     * @param transitions transition list which the newly created transitions should be appended to
     */
    public static void appendAllPossibleTransitionsFromStates(HashSet<MachineState> sourceStateSet, MachineState destinationState, ArrayList<Transition> transitions) {
        for (MachineState fromState: sourceStateSet) {
            transitions.add(new Transition(fromState, destinationState));
        }
    }

    /**
     * Creates a state machine with the given possible transitions. The machine's current state is set to the given state.
     * The given possible transitions are checked to detect conflicting transitions. Transitions <code>T1</code> and <code>T2</code>
     * are conflicting if <code>T1.sourceState == T2.sourceState</code>,
     * <code>T1.destinationState.load == T2.destinationState.load</code> and
     * <code>T1.destinationState.failoverActive != T2.destinationState.failoverActive</code>.
     *
     * @param transitionList possible transitions for the new machine
     * @param start start state for the new machine
     * @throws IllegalArgumentException if the given transition list contains conflicting transitions; or if the given start state is not
     * a source for any of the given transitions
     */
    public Machine(List<Transition> transitionList, MachineState start) {
        for (Transition t: transitionList) {
            if (!transitions.containsKey(t.getSourceState())) {
                transitions.put(t.getSourceState(), new HashMap<Load, Boolean>());
            }
            if (transitions.get(t.getSourceState()).containsKey(t.getDestinationState().getLoad())) {
                Load load = t.getDestinationState().getLoad();
                boolean failoverActive = transitions.get(t.getSourceState()).get(load);
                if (failoverActive != t.getDestinationState().isFailoverActive()) {
                    throw new IllegalArgumentException("transitionList contains two conflicting transitions");
                }
            } else {
                transitions.get(t.getSourceState()).put(t.getDestinationState().getLoad(), t.getDestinationState().isFailoverActive());
            }
        }
        if (!transitions.containsKey(start)) {
            throw new IllegalArgumentException("start not found");
        }
    }

    /**
     * Makes a transition from the given state <code>fromState</code> to such a state <code>toState</code> that
     * <code>toState.load == load</code>
     * and transition <code>fromState->toState</code> is among possible transitions of this machine
     * (possible transitions are specified during machine creation).
     *
     * @param fromState state that the transition should be made from
     * @param load input symbol that determines the transition
     * @return state after the transition
     */
    public MachineState transition(MachineState fromState, Load load) {
        Utils.assertNotNull(fromState, "fromState");
        Utils.assertNotNull(load, "load");
        ensureNoReturnToNotMeasuredYet(fromState.getLoad().getMainDb(), load.getMainDb(), false);
        ensureNoReturnToNotMeasuredYet(fromState.getLoad().getFailoverDb(), load.getFailoverDb(), true);

        Map<Load, Boolean> possibleTransitions = getPossibleTransitions(fromState);
        Boolean failoverActiveAfterTransition = possibleTransitions.get(load);
        if (failoverActiveAfterTransition != null) {
            return new MachineState(failoverActiveAfterTransition, load);
        } else {
            if (load.canBeGeneralized()) {
                return tryTransitionForMoreGeneralHadesLoad(fromState, load, possibleTransitions);
            } else {
                throw new IllegalStateException("transition from " + fromState + " through " + load + " not found");
            }
        }
    }

    private void ensureNoReturnToNotMeasuredYet(LoadLevel from, LoadLevel to, boolean failover) {
        if (to == LoadLevel.notMeasuredYet && from != LoadLevel.notMeasuredYet) {
            throw new IllegalArgumentException("return to notMeasuredYet for failover=" + failover);
        }
    }

    private MachineState tryTransitionForMoreGeneralHadesLoad(MachineState fromState, Load load, Map<Load, Boolean> possibleTransitions) {
        Load moreGeneralLoad = load.generalize();
        Boolean failoverActiveAfterTransition = possibleTransitions.get(moreGeneralLoad);
        if (failoverActiveAfterTransition != null) {
            return new MachineState(failoverActiveAfterTransition, moreGeneralLoad);
        } else {
            throw new IllegalStateException("transition from " + fromState + " through " + load + " nor through " + moreGeneralLoad + " not found");
        }
    }

    private Map<Load, Boolean> getPossibleTransitions(MachineState fromState) {
        Map<Load, Boolean> map = transitions.get(fromState);
        if (map != null) {
            return map;
        } else {
            throw new IllegalStateException("no transition from state " + fromState);
        }
    }
}
