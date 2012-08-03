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

        assert isLocalState() && !isLocalStateCombinedWithRemoteOne() && !isRemoteState();
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

        assert !isLocalState() && !isLocalStateCombinedWithRemoteOne() && isRemoteState();
    }

    private boolean isLocalState() {
        return loadFactory != null && period[mainIndex] != -1;
    }

    private boolean isLocalStateCombinedWithRemoteOne() {
        return loadFactory != null && period[mainIndex] == -1;
    }

    private boolean isRemoteState() {
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

    public void updateLocalStateWithNewExecTimes(String logPrefix, Hades hades, long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos, String quartzInstanceId) {
        if (!isLocalState()) {
            throw new IllegalStateException("this state must be a local one");
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
        this.machineState = stateMachine.transition(oldMachineState, this.load);
        this.quartzInstanceId = quartzInstanceId;

        updateCycleAndPeriod(logPrefix + hades.getDsName(false, true) + ": ", mainDbStmtExecTimeNanos, oldMachineState, mainIndex);
        updateCycleAndPeriod(logPrefix + hades.getDsName(true, true) + ": ", failoverDbStmtExecTimeNanos, oldMachineState, failoverIndex);
    }

    private void updateCycleAndPeriod(String logPrefix, long dbStmtExecTimeNanos, MachineState oldMachineState, int index) {
        if (dbStmtExecTimeNanos != notMeasuredInThisCycle) {
            if (dbStillNotUsedAndWithoutProblems(index, oldMachineState, this.machineState)) {
                keepDecreasedLoadOfUnusedDatabase(logPrefix, index);
            } else if (dbWithProblems(index, this.machineState)) {
                decreaseLoadOfDatabaseWithProblems(logPrefix, index);
            } else if (dbNotUsedAndWithoutProblems(index, this.machineState)) {
                decreaseLoadOfDatabaseThatBecameUnused(logPrefix, index);
            } else {
                keepNormalLoadOfUsedDatabase(logPrefix, index);
            }
        } else {
            cycleModuloPeriod[index] = increaseCycle(cycleModuloPeriod[index], period[index]);
        }
    }

    private void keepDecreasedLoadOfUnusedDatabase(String logPrefix, int index) {
        this.cycleModuloPeriod[index] = increaseCycle(cycleModuloPeriod[index], period[index]);
        logger.info(logPrefix + "db still unused: period=currentToUnusedRatio=" + currentToUnusedRatio);
    }

    private void decreaseLoadOfDatabaseWithProblems(String logPrefix, int index) {
        int oldPeriod = this.period[index];
        period[index] = oldPeriod * backOffMultiplier;
        if (period[index] > backOffMaxRatio) {
            period[index] = backOffMaxRatio;
        }
        cycleModuloPeriod[index] = increaseCycle(cycleModuloPeriod[index], period[index]);
        logger.info(logPrefix + "load level at least high: increasing period to decrease load: old period=" + oldPeriod + ", new period=" + cycleModuloPeriod[index]);
    }

    private void decreaseLoadOfDatabaseThatBecameUnused(String logPrefix, int index) {
        int oldPeriod = this.period[index];
        this.period[index] = currentToUnusedRatio;
        this.cycleModuloPeriod[index] = increaseCycle(0, this.period[index]);
        logger.info(logPrefix + "db became unused: old period=" + oldPeriod + ", new period=currentToUnusedRatio=" + currentToUnusedRatio);
    }

    private void keepNormalLoadOfUsedDatabase(String logPrefix, int index) {
        int oldPeriod = this.period[index];
        this.cycleModuloPeriod[index] = 0;
        this.period[index] = 1;
        if (oldPeriod > 1) {
            logger.info(logPrefix + "back to period=1 (old period=" + oldPeriod + ")");
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

    private boolean dbStillNotUsedAndWithoutProblems(int index, MachineState oldMachineState, MachineState newMachineState) {
        if (index == failoverIndex) {
            return !oldMachineState.isFailoverActive() && !newMachineState.isFailoverActive()
                   && (oldMachineState.getLoad().getFailoverDb() == LoadLevel.low || oldMachineState.getLoad().getFailoverDb() == LoadLevel.medium)
                   && (newMachineState.getLoad().getFailoverDb() == LoadLevel.low || newMachineState.getLoad().getFailoverDb() == LoadLevel.medium);
        } else {
            return oldMachineState.isFailoverActive() && newMachineState.isFailoverActive()
                   && (oldMachineState.getLoad().getMainDb() == LoadLevel.low || oldMachineState.getLoad().getMainDb() == LoadLevel.medium)
                   && (newMachineState.getLoad().getMainDb() == LoadLevel.low || newMachineState.getLoad().getMainDb() == LoadLevel.medium);
        }
    }

    private boolean dbNotUsedAndWithoutProblems(int index, MachineState newMachineState) {
        if (index == failoverIndex) {
            return !newMachineState.isFailoverActive() && (newMachineState.getLoad().getFailoverDb() == LoadLevel.low || newMachineState.getLoad().getFailoverDb() == LoadLevel.medium);
        } else {
            return newMachineState.isFailoverActive() && (newMachineState.getLoad().getMainDb() == LoadLevel.low || newMachineState.getLoad().getMainDb() == LoadLevel.medium);
        }
    }

    private boolean dbWithProblems(int index, MachineState newMachineState) {
        LoadLevel l;
        if (index == failoverIndex) {
            l = newMachineState.getLoad().getFailoverDb();
        } else {
            l = newMachineState.getLoad().getMainDb();
        }
        return l == LoadLevel.high || l == LoadLevel.exceptionWhileMeasuring;
    }

    public void copyFrom(String logPrefixIfLocalState, State localOrRemote) {
        if (isRemoteState()) {
            throw new IllegalStateException("this state must not be a remote one");
        }
        if (localOrRemote.isLocalStateCombinedWithRemoteOne()) {
            throw new IllegalStateException("state to copy from (localOrRemote) must not be a copy of a remote state");
        }

        this.modifyTimeMillis = localOrRemote.getModifyTimeMillis();
        this.load = localOrRemote.load;
        this.avg = localOrRemote.avg;
        this.avgFailover = localOrRemote.avgFailover;
        this.quartzInstanceId = localOrRemote.quartzInstanceId;
        this.machineState = localOrRemote.machineState;
        this.period[mainIndex] = localOrRemote.period[mainIndex];
        this.period[failoverIndex] = localOrRemote.period[failoverIndex];
        this.cycleModuloPeriod[mainIndex] = localOrRemote.cycleModuloPeriod[mainIndex];
        this.cycleModuloPeriod[failoverIndex] = localOrRemote.cycleModuloPeriod[failoverIndex];

        if (localOrRemote.isLocalState()) {
            this.history = localOrRemote.history.clone();
            this.historyFailover = localOrRemote.historyFailover.clone();
            checkConfigsIdentical(logPrefixIfLocalState, localOrRemote);
            assert isLocalState();
        } else {
            this.history = null;
            this.historyFailover = null;
            assert isLocalStateCombinedWithRemoteOne();
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
