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

import java.util.*;

/**
 * A deterministic finite-state machine that can be used to control failover activation.
 * States are of type {@link State}. The current state of the machine holds the current state of failover activation.
 * When an input symbol <code>L</code> of type {@link Load}
 * is provided a {@link Transition} <code>T</code> is made such that <code>T.sourceState</code> is equal to the current state,
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
public class StateMachine {

    private final Map<State, Map<Load, Boolean>> transitions = new HashMap<State, Map<Load, Boolean>>();
    private State currentState;

    /**
     * For the given set of states creates all possible transitions and appends them to the given transition list.
     *
     * @param states set of states for which all possible transitions should be created and added to the given transition list
     * @param transitionList list of transitions which should be extended with all possible transitions between given states
     */
    public static void appendAllPossibleTransitionsBetweenStates(Set<State> states, List<Transition> transitionList) {
        for (State state1 : states) {
            for (State state2 : states) {
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
    public static void appendAllPossibleTransitionsFromStates(HashSet<State> sourceStateSet, State destinationState, ArrayList<Transition> transitions) {
        for (State fromState: sourceStateSet) {
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
    public StateMachine(List<Transition> transitionList, State start) {
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
        currentState = start;
    }

    /**
     * Makes a transition from the current state <code>S1</code> to such a state <code>S2</code> that <code>S2.load == load</code>
     * and transition <code>S1->S2</code> is among possible transitions of this machine (possible transitions are specified
     * during machine creation).
     *
     * @param load input symbol that determines the transition
     * @return true if the given load causes a transition that indicates a failover or failback; false otherwise
     */
    public boolean transition(Load load) {
        Map<Load, Boolean> possibleTransitions = getPossibleTransitions();
        Boolean failoverActiveAfterTransition = possibleTransitions.get(load);
        if (failoverActiveAfterTransition != null) {
            return doTransition(new State(failoverActiveAfterTransition, load));
        } else {
            if (load.isMainDbLoadHigher() != null) {
                Load moreGeneralLoad = new Load(load.getMainDb());
                failoverActiveAfterTransition = possibleTransitions.get(moreGeneralLoad);
                if (failoverActiveAfterTransition != null) {
                    return doTransition(new State(failoverActiveAfterTransition, moreGeneralLoad));
                } else {
                    throw new IllegalStateException("transition from " + currentState + " through " + load + " nor " + moreGeneralLoad + " not found");
                }
            } else {
                throw new IllegalStateException("transition from " + currentState + " through " + load + " not found");
            }
        }
    }

    private boolean doTransition(State newState) {
        boolean failoverOrFailback = newState.isFailoverActive() != currentState.isFailoverActive();
        currentState = newState;
        return failoverOrFailback;
    }

    private Map<Load, Boolean> getPossibleTransitions() {
        Map<Load, Boolean> map = transitions.get(currentState);
        if (map != null) {
            return map;
        } else {
            throw new IllegalStateException("no transition from state " + currentState);
        }
    }

    public State getCurrentState() {
        return currentState;
    }
}
