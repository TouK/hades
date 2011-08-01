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

/**
 * An immutable state of a {@link StateMachine} consisting of a {@link Load} and a boolean indicating whether failover is active.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
class State {

    private final boolean failoverActive;
    private final Load load;

    public State(boolean failoverActive, Load load) {
        if (load == null) {
            throw new IllegalArgumentException("pair must not be null");
        }
        this.failoverActive = failoverActive;
        this.load = load;
    }

    public State(boolean failoverActive, LoadLevel mainDbLoadLevel, LoadLevel failoverDbLoadLevel) {
        this.failoverActive = failoverActive;
        this.load = new Load(mainDbLoadLevel, failoverDbLoadLevel);
    }

    public State(boolean failoverActive, LoadLevel loadLevel, boolean mainDbLoadHigher) {
        this.failoverActive = failoverActive;
        this.load = new Load(loadLevel, mainDbLoadHigher);
    }

    public State(boolean failoverActive, LoadLevel loadLevel) {
        this.failoverActive = failoverActive;
        this.load = new Load(loadLevel);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        State state = (State) o;

        if (failoverActive != state.failoverActive) return false;
        if (load != null ? !load.equals(state.load) : state.load != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (failoverActive ? 1 : 0);
        result = 31 * result + (load != null ? load.hashCode() : 0);
        return result;
    }

    public boolean isFailoverActive() {
        return failoverActive;
    }

    public Load getLoad() {
        return load;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("{failoverActive=").append(failoverActive).append(",").append(load);
        sb.append("}");
        return sb.toString();
    }
}
