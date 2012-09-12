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
import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;
import static pl.touk.hades.load.LoadLevel.*;

import pl.touk.hades.load.LoadLevel;
import pl.touk.hades.load.statemachine.MachineState;
import pl.touk.hades.load.statemachine.Machine;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public final class State implements Serializable, Cloneable {

    private final static Logger logger = LoggerFactory.getLogger(State.class);

    private final static long serialVersionUID = 7038908200270943595L;

    public final static long notMeasuredInThisCycle = -1L;

    private static final Machine stateMachine = Machine.createStateMachine();

    private long modifyTimeMillis;
    private Load load;
    private Average avg;
    private Average avgFailover;
    private String host;
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
    private final String mainDbName;
    private final String failoverDbName;

    public State(SqlTimeBasedLoadFactory loadFactory,
                 String host,
                 int sqlTimesIncludedInAverage,
                 boolean exceptionsIgnoredAfterRecovery,
                 boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                 int currentToUnusedRatio,
                 int backOffMultiplier,
                 int backOffMaxRatio,
                 String mainDbName,
                 String failoverDbName) {

        Utils.assertNotNull(loadFactory, "loadFactory");
        Utils.assertPositive(currentToUnusedRatio, "currentToUnusedRatio");
        Utils.assertPositive(backOffMultiplier, "backOffMultiplier");
        Utils.assertPositive(backOffMaxRatio, "backOffMaxRatio");

        this.host = host;
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
        this.mainDbName = mainDbName;
        this.failoverDbName = failoverDbName;

        assert isLocalState() && !isLocalStateCombinedWithRemoteOne() && !isRemoteState();
    }

    public State(String host,
                 long modifyTimeMillis,
                 boolean failover,
                 long lastMainQueryTimeNanos,
                 long lastFailoverQueryTimeNanos) {

        this.host = host;
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
        this.mainDbName = null;
        this.failoverDbName = null;

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

    public String getHost() {
        return host;
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
        if (host != null ? !host.equals(state.host) : state.host != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (modifyTimeMillis ^ (modifyTimeMillis >>> 32));
        result = 31 * result + (load != null ? load.hashCode() : 0);
        result = 31 * result + (avg != null ? avg.hashCode() : 0);
        result = 31 * result + (avgFailover != null ? avgFailover.hashCode() : 0);
        result = 31 * result + (host != null ? host.hashCode() : 0);
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
                ", host='" + host + '\'' +
                ", history=" + history +
                ", historyFailover=" + historyFailover +
                ", machineState=" + machineState +
                ", period=" + Arrays.toString(period) +
                ", cycleModuloPeriod=" + Arrays.toString(cycleModuloPeriod) +
                ", currentToUnusedRatio=" + currentToUnusedRatio +
                ", backOffMultiplier=" + backOffMultiplier +
                ", backOffMaxRatio=" + backOffMaxRatio +
                ", loadFactory=" + loadFactory +
                '}';
    }

    public void updateLocalStateWithNewExecTimes(String logPrefix,
                                                 long mainDbStmtExecTimeNanos,
                                                 long failoverDbStmtExecTimeNanos,
                                                 String host) {
        long time;
        Average newAvg;
        Average newAvgFailover;
        SqlTimeHistory oldHistory = history.clone();
        SqlTimeHistory oldHistoryFailover = historyFailover.clone();
        MachineState newMachineState;
        int[] mainCycleAndPeriod;
        int[] failoverCycleAndPeriod;

        try {
            time = System.currentTimeMillis();

            if (!isLocalState()) {
                throw new IllegalStateException("this.updateLocalStateWithNewExecTimes method can be " +
                        "invoked only when this.isLocalState() is true but this is not the case: this=" + this);
            }

            newAvg = avg;
            newAvgFailover = avgFailover;
            if (validateSqlTime(false, mainDbStmtExecTimeNanos)) {
                newAvg = history.updateAverage(mainDbStmtExecTimeNanos);
            }
            if (validateSqlTime(true, failoverDbStmtExecTimeNanos)) {
                newAvgFailover = historyFailover.updateAverage(failoverDbStmtExecTimeNanos);
            }

            Load load = loadFactory.getLoad(newAvg.getValue(), newAvgFailover.getValue());
            MachineState oldMachineState = machineState;
            newMachineState = stateMachine.transition(oldMachineState, load);

            mainCycleAndPeriod = getUpdatedCycleAndPeriod(
                    logPrefix + mainDbName + ": ",
                    mainDbStmtExecTimeNanos,
                    oldMachineState,
                    newMachineState,
                    mainIndex);
            failoverCycleAndPeriod = getUpdatedCycleAndPeriod(
                    logPrefix + failoverDbName + ": ",
                    failoverDbStmtExecTimeNanos,
                    oldMachineState,
                    newMachineState,
                    failoverIndex);
        } catch (RuntimeException e) {
            history = oldHistory;
            historyFailover = oldHistoryFailover;
            return;
        }

        updateState(time, newAvg, newAvgFailover, load, newMachineState, mainCycleAndPeriod, failoverCycleAndPeriod, host);
    }

    private void updateState(long time,
                             Average newAvg,
                             Average newAvgFailover,
                             Load load,
                             MachineState newMachineState,
                             int[] mainCycleAndPeriod,
                             int[] failoverCycleAndPeriod,
                             String host) {
        this.modifyTimeMillis = time;
        this.avg = newAvg;
        this.avgFailover = newAvgFailover;
        this.load = load;
        this.machineState = newMachineState;
        this.cycleModuloPeriod[mainIndex] = mainCycleAndPeriod[0];
        this.period[mainIndex] = mainCycleAndPeriod[1];
        this.cycleModuloPeriod[failoverIndex] = failoverCycleAndPeriod[0];
        this.period[failoverIndex] = failoverCycleAndPeriod[1];
        this.host = host;
    }

    private boolean validateSqlTime(boolean failover, long sqlTimeNanos) {
        if (sqlTimeNanos != notMeasuredInThisCycle) {
            Utils.assertNonNegative(sqlTimeNanos, "sqlTimeNanos");
            Utils.assertSame(0, cycleModuloPeriod(failover), "given sql time ("
                    + ExceptionEnum.erroneousValuesAsStr(sqlTimeNanos) + ") indicates that the "
                    + (failover ? "failover" : "main")
                    + " db load was measured; this should not be the case as its cycleModuloPeriod != 0");
            return true;
        } else {
            if (cycleModuloPeriod(failover) <= 0) {
                throw new IllegalStateException("sql time was not measured in this cycle for "
                        + (failover ? "failover" : "main")
                        + " db; hence cycleModuloPeriod for this db should be greater than zero but it isn't: "
                        + cycleModuloPeriod(failover));
            }
            return false;
        }
    }

    private int cycleModuloPeriod(boolean failover) {
        return cycleModuloPeriod[failover ? failoverIndex : mainIndex];
    }

    private int[] getUpdatedCycleAndPeriod(String logPrefix,
                                           long dbStmtExecTimeNanos,
                                           MachineState oldMachineState,
                                           MachineState newMachineState,
                                           int index) {
        if (dbStmtExecTimeNanos != notMeasuredInThisCycle) {
            if (dbStillNotUsedAndWithoutProblems(index, oldMachineState, newMachineState)) {
                return keepDecreasedLoadOfUnusedDatabase(logPrefix, index);
            } else if (dbWithProblems(index, newMachineState)) {
                return decreaseLoadOfDatabaseWithProblems(logPrefix, index);
            } else if (dbNotUsed(index, newMachineState)) {
                assertDbWithoutProblems(index, newMachineState);
                return decreaseLoadOfDatabaseThatBecameUnused(logPrefix, index);
            } else {
                assertDbWithoutProblems(index, newMachineState);
                return keepNormalLoadOfUsedDatabase(logPrefix, index);
            }
        } else {
            return getArrayWithIncreasedCycleModuloPeriodAndGivenPeriod(cycleModuloPeriod[index], period[index]);
        }
    }

    private void assertDbWithoutProblems(int index, MachineState newMachineState) {
        if (newMachineState.getLoad().getLoadLevel(index == failoverIndex) != low
                && newMachineState.getLoad().getLoadLevel(index == failoverIndex) != medium) {
            throw new IllegalStateException((index == failoverIndex ? "failover" : "main")
                    + " db should not have problems");
        }
    }

    private int[] keepDecreasedLoadOfUnusedDatabase(String logPrefix, int index) {
        if (period[index] != currentToUnusedRatio) {
            throw new IllegalStateException("cycle[" + index + "] != currentToUnusedRatio (" + currentToUnusedRatio + ")");
        }
        logger.info(logPrefix + "db still unused: period=currentToUnusedRatio=" + currentToUnusedRatio);
        return getArrayWithIncreasedCycleModuloPeriodAndGivenPeriod(cycleModuloPeriod[index], currentToUnusedRatio);
    }

    private int[] decreaseLoadOfDatabaseWithProblems(String logPrefix, int index) {
        int oldPeriod = this.period[index];
        int newPeriod = oldPeriod * backOffMultiplier;
        if (newPeriod > backOffMaxRatio) {
            newPeriod = backOffMaxRatio;
        }
        logger.info(logPrefix + "load level at least high: increasing period to decrease load: old period=" + oldPeriod + ", new period=" + newPeriod);
        return getArrayWithIncreasedCycleModuloPeriodAndGivenPeriod(cycleModuloPeriod[index], newPeriod);
    }

    private int[] decreaseLoadOfDatabaseThatBecameUnused(String logPrefix, int index) {
        int oldPeriod = this.period[index];
        int oldCycle = cycleModuloPeriod[index];
        if (oldPeriod != 1 || oldCycle != 0) {
            throw new IllegalStateException("decreaseLoadOfDatabaseThatBecameUnused method used but period[" + index
                    + "] != 1 (" + oldPeriod + ") or cycleModuloPeriod[" + index + "] != 0 (" + oldCycle + ")");
        }
        logger.info(logPrefix + "db became unused: old period=" + oldPeriod + ", new period=currentToUnusedRatio=" + currentToUnusedRatio);
        return getArrayWithIncreasedCycleModuloPeriodAndGivenPeriod(0, currentToUnusedRatio);
    }

    private int[] keepNormalLoadOfUsedDatabase(String logPrefix, int index) {
        if (this.period[index] > 1) {
            logger.info(logPrefix + "back to period=1 (old period=" + this.period[index]
                    + ") for " + (index == failoverIndex ? "failover" : "main") + " db");
        }
        return new int[]{0, 1};
    }

    private int[] getArrayWithIncreasedCycleModuloPeriodAndGivenPeriod(int cycleModuloPeriod, int period) {
        cycleModuloPeriod++;
        if (cycleModuloPeriod < period) {
            return new int[]{cycleModuloPeriod, period};
        } else {
            return new int[]{0, period};
        }
    }

    private boolean dbStillNotUsedAndWithoutProblems(int index, MachineState oldMachineState, MachineState newMachineState) {
        if (index == failoverIndex) {
            return !oldMachineState.isFailoverActive() && !newMachineState.isFailoverActive()
                   && (oldMachineState.getLoad().getFailoverDb() == low || oldMachineState.getLoad().getFailoverDb() == medium)
                   && (newMachineState.getLoad().getFailoverDb() == low || newMachineState.getLoad().getFailoverDb() == medium);
        } else {
            return oldMachineState.isFailoverActive() && newMachineState.isFailoverActive()
                   && (oldMachineState.getLoad().getMainDb() == low || oldMachineState.getLoad().getMainDb() == medium)
                   && (newMachineState.getLoad().getMainDb() == low || newMachineState.getLoad().getMainDb() == medium);
        }
    }

    private boolean dbNotUsed(int index, MachineState newMachineState) {
        if (index == failoverIndex) {
            return !newMachineState.isFailoverActive() && (newMachineState.getLoad().getFailoverDb() == low || newMachineState.getLoad().getFailoverDb() == medium);
        } else {
            return newMachineState.isFailoverActive() && (newMachineState.getLoad().getMainDb() == low || newMachineState.getLoad().getMainDb() == medium);
        }
    }

    private boolean dbWithProblems(int index, MachineState newMachineState) {
        LoadLevel l = newMachineState.getLoad().getLoadLevel(index == failoverIndex);
        return l == high || l == exceptionWhileMeasuring;
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
        this.host = localOrRemote.host;
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
        if (loadFactory.getFailoverThresholdNanos() != fullState.loadFactory.getFailoverThresholdNanos()
                || loadFactory.getFailbackThresholdNanos() != fullState.loadFactory.getFailbackThresholdNanos()
                || currentToUnusedRatio != fullState.currentToUnusedRatio
                || backOffMultiplier != fullState.backOffMultiplier
                || backOffMaxRatio != fullState.backOffMaxRatio) {
            logger.warn(logPrefix + "config difference detected; local state:\n" + this + "\nremote state:\n" + fullState);
        }

    }

    public boolean sqlTimeIsMeasuredInThisCycle(boolean failover) {
        return cycleModuloPeriod[failover ? failoverIndex : mainIndex] == 0;
    }

    public int getPeriod(boolean failover) {
        return period[failover ? failoverIndex : mainIndex];
    }

    public int getCycleModuloPeriod(boolean failover) {
        return cycleModuloPeriod[failover ? failoverIndex : mainIndex];
    }
}
