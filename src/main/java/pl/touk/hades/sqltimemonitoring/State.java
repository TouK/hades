/*
 * Copyright 2012 TouK
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
package pl.touk.hades.sqltimemonitoring;

import pl.touk.hades.load.HadesLoad;
import pl.touk.hades.load.statemachine.MachineState;
import pl.touk.hades.load.statemachine.Machine;

import java.io.Serializable;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class State implements Serializable, Cloneable {

    private static final long serialVersionUID = 7038908200270943595L;

    private static final Machine stateMachine = Machine.createStateMachine();

    private final SqlTimeBasedHadesLoadFactory loadFactory;

    private HadesLoad load;
    private Average avg;
    private Average avgFailover;
    private MachineState machineState = Machine.initialState;
    private SqlTimeHistory history;
    private SqlTimeHistory historyFailover;

    public State() {
        loadFactory = null;
    }

    public State(SqlTimeBasedHadesLoadFactory loadFactory) {
        this.loadFactory = loadFactory;
    }

    @Override
    public State clone() {
        try {
            State copy = (State) super.clone();
            copy.history = history.clone();
            copy.historyFailover = historyFailover.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public HadesLoad getLoad() {
        return load;
    }

    public Average getAvg() {
        return avg;
    }

    public void setAvg(Average avg) {
        this.avg = avg;
    }

    public Average getAvgFailover() {
        return avgFailover;
    }

    public MachineState getMachineState() {
        return machineState;
    }

    public void setMachineState(MachineState machineState) {
        this.machineState = machineState;
    }

    public SqlTimeHistory getHistory() {
        return history;
    }

    public void setHistory(SqlTimeHistory history) {
        this.history = history;
    }

    public SqlTimeHistory getHistoryFailover() {
        return historyFailover;
    }

    public void setHistoryFailover(SqlTimeHistory historyFailover) {
        this.historyFailover = historyFailover;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        State state = (State) o;

        if (avg != null ? !avg.equals(state.avg) : state.avg != null) return false;
        if (avgFailover != null ? !avgFailover.equals(state.avgFailover) : state.avgFailover != null) return false;
        if (history != null ? !history.equals(state.history) : state.history != null) return false;
        if (historyFailover != null ? !historyFailover.equals(state.historyFailover) : state.historyFailover != null)
            return false;
        if (load != null ? !load.equals(state.load) : state.load != null) return false;
        if (loadFactory != null ? !loadFactory.equals(state.loadFactory) : state.loadFactory != null) return false;
        if (machineState != null ? !machineState.equals(state.machineState) : state.machineState != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = loadFactory != null ? loadFactory.hashCode() : 0;
        result = 31 * result + (load != null ? load.hashCode() : 0);
        result = 31 * result + (avg != null ? avg.hashCode() : 0);
        result = 31 * result + (avgFailover != null ? avgFailover.hashCode() : 0);
        result = 31 * result + (machineState != null ? machineState.hashCode() : 0);
        result = 31 * result + (history != null ? history.hashCode() : 0);
        result = 31 * result + (historyFailover != null ? historyFailover.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "State{" +
                ", load=" + load +
                ", avg=" + avg +
                ", avgFailover=" + avgFailover +
                ", machineState=" + machineState +
                ", history=" + history +
                ", historyFailover=" + historyFailover +
                '}';
    }

    public void update(long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos) {
        avg = history.updateAverage(mainDbStmtExecTimeNanos);
        avgFailover = historyFailover.updateAverage(failoverDbStmtExecTimeNanos);
        load = loadFactory.getLoad(avg.getValue(), avgFailover.getValue());
        MachineState oldMachineState = machineState;
        machineState = stateMachine.transition(oldMachineState, load);
    }

    public void createHistories(int sqlTimesIncludedInAverage, boolean exceptionsIgnoredAfterRecovery, boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery) {
        history         = new SqlTimeHistory(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        historyFailover = new SqlTimeHistory(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
    }
}
