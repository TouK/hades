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
package pl.touk.hades.sql.timemonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;
import pl.touk.hades.load.LoadLevel;
import pl.touk.hades.load.statemachine.MachineState;
import pl.touk.hades.load.statemachine.Machine;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class State implements Serializable, Cloneable {

    private final static Logger logger = LoggerFactory.getLogger(State.class);

    private final static long serialVersionUID = 7038908200270943595L;

    public final static long notMeasuredInThisCycle = -1L;

    private static final Machine stateMachine = Machine.createStateMachine();

    private long modifyTimeMillis;
    private Load load;
    private Average avg;
    private Average avgFailover;
    private String quartzInstanceId;
    private SqlTimeHistory history;
    private SqlTimeHistory historyFailover;
    private MachineState machineState = Machine.initialState;

    private final SqlTimeBasedLoadFactory loadFactory;

    private final static int mainIndex = 0;
    private final static int failoverIndex = 1;
    private final int[] period = new int[2];
    private final int[] cycleModuloPeriod = new int[2];
    private final int currentToUnusedRatio;
    private final int backOffMultiplier;
    private final int backOffMaxRatio;

    public State(SqlTimeBasedLoadFactory loadFactory,
                 String quartzInstanceId,
                 int sqlTimesIncludedInAverage,
                 boolean exceptionsIgnoredAfterRecovery,
                 boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                 int currentToUnusedRatio,
                 int backOffMultiplier,
                 int backOffMaxRatio) {
        Utils.assertNotNull(loadFactory, "loadFactory");
        Utils.assertPositive(currentToUnusedRatio, "currentToUnusedRatio");
        Utils.assertPositive(backOffMultiplier, "backOffMultiplier");
        Utils.assertPositive(backOffMaxRatio, "backOffMaxRatio");

        this.quartzInstanceId = quartzInstanceId;
        this.modifyTimeMillis = System.currentTimeMillis();
        this.avg = null;
        this.avgFailover = null;
        this.history = new SqlTimeHistory(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        this.historyFailover = new SqlTimeHistory(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        this.machineState = Machine.initialState;

        this.loadFactory = loadFactory;
        this.period[mainIndex] = 1;
        this.period[failoverIndex] = 1;
        this.cycleModuloPeriod[mainIndex] = 0;
        this.cycleModuloPeriod[failoverIndex] = 0;
        this.currentToUnusedRatio = currentToUnusedRatio;
        this.backOffMultiplier = backOffMultiplier;
        this.backOffMaxRatio = backOffMaxRatio;

        assert isFullState() && !isCopiedFromPartialState() && !isPartialState();
    }

    public State(String instanceId, long modifyTimeMillis, boolean failover, long lastMainQueryTimeNanos, long lastFailoverQueryTimeNanos) {
        this.quartzInstanceId = instanceId;
        this.modifyTimeMillis = modifyTimeMillis;
        this.avg = new Average(lastMainQueryTimeNanos, 1, lastMainQueryTimeNanos);
        this.avgFailover = new Average(lastFailoverQueryTimeNanos, 1, lastFailoverQueryTimeNanos);
        this.history = null;
        this.historyFailover = null;
        this.machineState = new MachineState(failover, (Load) null);

        this.loadFactory = null;
        this.period[mainIndex] = -1;
        this.period[failoverIndex] = -1;
        this.cycleModuloPeriod[mainIndex] = -1;
        this.cycleModuloPeriod[failoverIndex] = -1;
        this.currentToUnusedRatio = -1;
        this.backOffMultiplier = -1;
        this.backOffMaxRatio = -1;

        assert !isFullState() && !isCopiedFromPartialState() && isPartialState();
    }

    private boolean isFullState() {
        return loadFactory != null && period[mainIndex] != -1;
    }

    private boolean isCopiedFromPartialState() {
        return loadFactory != null && period[mainIndex] == -1;
    }

    private boolean isPartialState() {
        return loadFactory == null && period[mainIndex] == -1;
    }

    @Override
    public State clone() {
        try {
            State copy = (State) super.clone();
            if (history != null) {
                copy.history = history.clone();
            }
            if (historyFailover != null) {
                copy.historyFailover = historyFailover.clone();
            }
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public long getModifyTimeMillis() {
        return modifyTimeMillis;
    }

    public Average getAvg() {
        return avg;
    }

    public Average getAvgFailover() {
        return avgFailover;
    }

    public MachineState getMachineState() {
        return machineState;
    }

    public String getQuartzInstanceId() {
        return quartzInstanceId;
    }

    // equals and hashCode methods auto-generated using all fields:
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        State state = (State) o;

        if (backOffMaxRatio != state.backOffMaxRatio) return false;
        if (backOffMultiplier != state.backOffMultiplier) return false;
        if (currentToUnusedRatio != state.currentToUnusedRatio) return false;
        if (modifyTimeMillis != state.modifyTimeMillis) return false;
        if (avg != null ? !avg.equals(state.avg) : state.avg != null) return false;
        if (avgFailover != null ? !avgFailover.equals(state.avgFailover) : state.avgFailover != null) return false;
        if (!Arrays.equals(cycleModuloPeriod, state.cycleModuloPeriod)) return false;
        if (history != null ? !history.equals(state.history) : state.history != null) return false;
        if (historyFailover != null ? !historyFailover.equals(state.historyFailover) : state.historyFailover != null)
            return false;
        if (load != null ? !load.equals(state.load) : state.load != null) return false;
        if (loadFactory != null ? !loadFactory.equals(state.loadFactory) : state.loadFactory != null) return false;
        if (machineState != null ? !machineState.equals(state.machineState) : state.machineState != null) return false;
        if (!Arrays.equals(period, state.period)) return false;
        if (quartzInstanceId != null ? !quartzInstanceId.equals(state.quartzInstanceId) : state.quartzInstanceId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (modifyTimeMillis ^ (modifyTimeMillis >>> 32));
        result = 31 * result + (load != null ? load.hashCode() : 0);
        result = 31 * result + (avg != null ? avg.hashCode() : 0);
        result = 31 * result + (avgFailover != null ? avgFailover.hashCode() : 0);
        result = 31 * result + (quartzInstanceId != null ? quartzInstanceId.hashCode() : 0);
        result = 31 * result + (history != null ? history.hashCode() : 0);
        result = 31 * result + (historyFailover != null ? historyFailover.hashCode() : 0);
        result = 31 * result + (machineState != null ? machineState.hashCode() : 0);
        result = 31 * result + (loadFactory != null ? loadFactory.hashCode() : 0);
        result = 31 * result + (period != null ? Arrays.hashCode(period) : 0);
        result = 31 * result + (cycleModuloPeriod != null ? Arrays.hashCode(cycleModuloPeriod) : 0);
        result = 31 * result + currentToUnusedRatio;
        result = 31 * result + backOffMultiplier;
        result = 31 * result + backOffMaxRatio;
        return result;
    }

    // toString() auto-generated using all fields:
    @Override
    public String toString() {
        return "State{" +
                "modifyTimeMillis=" + modifyTimeMillis +
                ", load=" + load +
                ", avg=" + avg +
                ", avgFailover=" + avgFailover +
                ", quartzInstanceId='" + quartzInstanceId + '\'' +
                ", history=" + history +
                ", historyFailover=" + historyFailover +
                ", machineState=" + machineState +
                ", period=" + period +
                ", cycleModuloPeriod=" + cycleModuloPeriod +
                ", currentToUnusedRatio=" + currentToUnusedRatio +
                ", backOffMultiplier=" + backOffMultiplier +
                ", backOffMaxRatio=" + backOffMaxRatio +
                ", loadFactory=" + loadFactory +
                '}';
    }

    public void updateFullState(String logPrefix, Hades hades, long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos, String quartzInstanceId) {
        if (!isFullState()) {
            throw new IllegalStateException("this state is a partial state or a copy of a partial state hence should not be updated");
        }

        this.modifyTimeMillis = System.currentTimeMillis();

        if (mainDbStmtExecTimeNanos != notMeasuredInThisCycle) {
            Utils.assertNonNegative(mainDbStmtExecTimeNanos, "mainDbStmtExecTimeNanos");
            this.avg = history.updateAverage(mainDbStmtExecTimeNanos);
        }
        if (failoverDbStmtExecTimeNanos != notMeasuredInThisCycle) {
            Utils.assertNonNegative(failoverDbStmtExecTimeNanos, "failoverDbStmtExecTimeNanos");
            this.avgFailover = historyFailover.updateAverage(failoverDbStmtExecTimeNanos);
        }

        this.load = loadFactory.getLoad(avg.getValue(), avgFailover.getValue());
        MachineState oldMachineState = machineState;
        this.machineState = stateMachine.transition(oldMachineState, load);
        this.quartzInstanceId = quartzInstanceId;
        updateCycleAndPeriod(logPrefix + hades.getDsName(false, true) + ": ", mainDbStmtExecTimeNanos, oldMachineState, mainIndex);
        updateCycleAndPeriod(logPrefix + hades.getDsName(true, true) + ": ", failoverDbStmtExecTimeNanos, oldMachineState, failoverIndex);
    }

    private void updateCycleAndPeriod(String logPrefix, long dbStmtExecTimeNanos, MachineState oldMachineState, int i) {
        if (dbStmtExecTimeNanos != notMeasuredInThisCycle) {
            int oldPeriod = this.period[i];
            if (dbStillNotUsedAndWithoutProblems(i == failoverIndex, oldMachineState, this.machineState)) {
                this.cycleModuloPeriod[i] = increaseCycle(this.cycleModuloPeriod[i], oldPeriod);
                logger.info(logPrefix + "db still unused: period=currentToUnusedRatio=" + currentToUnusedRatio);
            } else if (dbWithProblems(i == failoverIndex, this.machineState)) {
                this.period[i] = oldPeriod * this.backOffMultiplier;
                if (this.period[i] > this.backOffMaxRatio) {
                    this.period[i] = this.backOffMaxRatio;
                }
                this.cycleModuloPeriod[i] = increaseCycle(this.cycleModuloPeriod[i], this.period[i]);
                logger.info(logPrefix + "load level at least high: increasing period to decrease load: old period=" + oldPeriod + ", new period=" + this.cycleModuloPeriod[i]);
            } else if (dbNotUsedAndWithoutProblems(i == failoverIndex, this.machineState)) {
                this.period[i] = currentToUnusedRatio;
                this.cycleModuloPeriod[i] = increaseCycle(0, this.period[i]);
                logger.info(logPrefix + "db became unused: old period=" + oldPeriod + ", new period=currentToUnusedRatio=" + currentToUnusedRatio);
            } else {
                this.cycleModuloPeriod[i] = 0;
                this.period[i] = 1;
                if (oldPeriod > 1) {
                    logger.info(logPrefix + "back to period=1 (old period=" + oldPeriod + ")");
                }
            }
        } else {
            this.cycleModuloPeriod[i] = increaseCycle(this.cycleModuloPeriod[i], this.period[i]);
        }

    }

    private int increaseCycle(int cycleModuloPeriod, int period) {
        cycleModuloPeriod++;
        if (cycleModuloPeriod < period) {
            return cycleModuloPeriod;
        } else {
            return 0;
        }
    }

    private boolean dbStillNotUsedAndWithoutProblems(boolean failover, MachineState oldMachineState, MachineState newMachineState) {
        if (failover) {
            return !oldMachineState.isFailoverActive() && !newMachineState.isFailoverActive()
                   && (oldMachineState.getLoad().getFailoverDb() == LoadLevel.low || oldMachineState.getLoad().getFailoverDb() == LoadLevel.medium)
                   && (newMachineState.getLoad().getFailoverDb() == LoadLevel.low || newMachineState.getLoad().getFailoverDb() == LoadLevel.medium);
        } else {
            return oldMachineState.isFailoverActive() && newMachineState.isFailoverActive()
                   && (oldMachineState.getLoad().getMainDb() == LoadLevel.low || oldMachineState.getLoad().getMainDb() == LoadLevel.medium)
                   && (newMachineState.getLoad().getMainDb() == LoadLevel.low || newMachineState.getLoad().getMainDb() == LoadLevel.medium);
        }
    }

    private boolean dbNotUsedAndWithoutProblems(boolean failover, MachineState newMachineState) {
        if (failover) {
            return !newMachineState.isFailoverActive() && (newMachineState.getLoad().getFailoverDb() == LoadLevel.low || newMachineState.getLoad().getFailoverDb() == LoadLevel.medium);
        } else {
            return newMachineState.isFailoverActive() && (newMachineState.getLoad().getMainDb() == LoadLevel.low || newMachineState.getLoad().getMainDb() == LoadLevel.medium);
        }
    }

    private boolean dbWithProblems(boolean failover, MachineState newMachineState) {
        LoadLevel l;
        if (failover) {
            l = newMachineState.getLoad().getFailoverDb();
        } else {
            l = newMachineState.getLoad().getMainDb();
        }
        return l == LoadLevel.high || l == LoadLevel.exceptionWhileMeasuring;
    }

    public void copyFrom(String logPrefixIfFullState, State fullOrPartialState) {
        if (isPartialState()) {
            throw new IllegalStateException("this state must not be a partial state");
        }
        if (fullOrPartialState.isCopiedFromPartialState()) {
            throw new IllegalStateException("state to copy from (fullOrPartialState) must not be a copy of a remote state");
        }

        this.modifyTimeMillis = fullOrPartialState.getModifyTimeMillis();
        this.load = fullOrPartialState.load;
        this.avg = fullOrPartialState.avg;
        this.avgFailover = fullOrPartialState.avgFailover;
        this.quartzInstanceId = fullOrPartialState.quartzInstanceId;
        this.machineState = fullOrPartialState.machineState;
        this.period[mainIndex] = fullOrPartialState.period[mainIndex];
        this.period[failoverIndex] = fullOrPartialState.period[failoverIndex];
        this.cycleModuloPeriod[mainIndex] = fullOrPartialState.cycleModuloPeriod[mainIndex];
        this.cycleModuloPeriod[failoverIndex] = fullOrPartialState.cycleModuloPeriod[failoverIndex];

        if (fullOrPartialState.isFullState()) {
            this.history = fullOrPartialState.history.clone();
            this.historyFailover = fullOrPartialState.historyFailover.clone();
            checkConfigsIdentical(logPrefixIfFullState, fullOrPartialState);
            assert isFullState();
        } else {
            this.history = null;
            this.historyFailover = null;
            assert isCopiedFromPartialState();
        }
    }

    private void checkConfigsIdentical(String logPrefix, State fullState) {
        if (loadFactory.getSqlTimeTriggeringFailoverNanos() != fullState.loadFactory.getSqlTimeTriggeringFailoverNanos()
                || loadFactory.getSqlTimeTriggeringFailbackNanos() != fullState.loadFactory.getSqlTimeTriggeringFailbackNanos()
                || currentToUnusedRatio != fullState.currentToUnusedRatio
                || backOffMultiplier != fullState.backOffMultiplier
                || backOffMaxRatio != fullState.backOffMaxRatio) {
            logger.warn(logPrefix + "config difference detected; local state:\n" + this + "\nremote state:\n" + fullState);
        }

    }

    public boolean sqlTimeIsMeasuredInThisCycle(boolean failover) {
        return cycleModuloPeriod[failover ? failoverIndex : mainIndex] == 0;
    }
}
