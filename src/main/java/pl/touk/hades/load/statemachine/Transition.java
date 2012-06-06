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

/**
 * A transition between states of a {@link Machine}.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class Transition {

    private final MachineState sourceState;
    private final MachineState destinationState;

    public Transition(MachineState sourceState, MachineState destinationState) {
        if (sourceState == null || destinationState == null) {
            throw new IllegalArgumentException("sourceState and destinationState must not be null");
        }
        this.sourceState = sourceState;
        this.destinationState = destinationState;
    }

    public MachineState getSourceState() {
        return sourceState;
    }

    public MachineState getDestinationState() {
        return destinationState;
    }
}
