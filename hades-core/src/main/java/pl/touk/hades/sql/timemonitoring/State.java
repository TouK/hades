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
    public  final static long notMeasuredInThisCycle = -1L;
    private final static Machine stateMachine = Machine.createStateMachine();
    private final static int mainIndex = 0;
    private final static int failoverIndex = 1;

    // All fields that are set during construction of remote states.
    // For local states they are also set during construction.
    private String host;
    private String repoId;
    private long modifyTimeMillis;
    private Average avg;
    private Average avgFailover;
    private MachineState machineState = Machine.initialState;

    // Fields that are set during construction of local states only.
    private SqlTimeHistory history;
    private SqlTimeHistory historyFailover;
    private final SqlTimeBasedLoadFactory loadFactory;
    private int[] period = new int[2];
    private int[] cycleModuloPeriod = new int[2];
    private final int currentToUnusedRatio;
    private final int backOffMultiplier;
    private final int backOffMaxRatio;
    private final String mainDbName;
    private final String failoverDbName;

    // Fields that are set only for local states and only during update with new exec times:
    private Load loadAfterLastMeasurement = null;
    private StateAfterMeasurement[] stateAfterLastMeasurement = new StateAfterMeasurement[]{null, null};

    private enum StateAfterMeasurement {
        notOkDb          ("db load level is " + LoadLevel.high + " or " + LoadLevel.exceptionWhileMeasuring),
        okDbStillNotUsed ("db is ok and still not used"),
        okDbBecameUnused ("db is ok and became unused"),
        okDbUsed         ("db is ok and used");

        final String desc;

        private StateAfterMeasurement(String desc) {
            this.desc = desc;
        }

        @Override
        public String toString() {
            return desc;
        }
    }

    public State(SqlTimeBasedLoadFactory loadFactory,
                 String host,
                 String repoId,
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
        this.repoId = repoId;
        this.modifyTimeMillis = System.currentTimeMillis();
        this.avg = null;
        this.avgFailover = null;
        this.machineState = Machine.initialState;

        this.history = new SqlTimeHistory(
                sqlTimesIncludedInAverage,
                exceptionsIgnoredAfterRecovery,
                recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        this.historyFailover = new SqlTimeHistory(
                sqlTimesIncludedInAverage,
                exceptionsIgnoredAfterRecovery,
                recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        this.loadFactory = loadFactory;
        this.setPeriod(false, 1);
        this.setPeriod(true, 1);
        this.setCycleModuloPeriod(false, 0);
        this.setCycleModuloPeriod(true, 0);
        this.currentToUnusedRatio = currentToUnusedRatio;
        this.backOffMultiplier = backOffMultiplier;
        this.backOffMaxRatio = backOffMaxRatio;
        this.mainDbName = mainDbName;
        this.failoverDbName = failoverDbName;

        assert isLocalState() && !isLocalStateCombinedWithRemoteOne() && !isRemoteState();
    }

    public State(String host,
                 String repoId,
                 long modifyTimeMillis,
                 boolean failover,
                 long lastMainQueryTimeNanos,
                 long lastFailoverQueryTimeNanos) {

        this.host = host;
        this.repoId = repoId;
        this.modifyTimeMillis = modifyTimeMillis;
        this.avg = new Average(lastMainQueryTimeNanos, 1, lastMainQueryTimeNanos);
        this.avgFailover = new Average(lastFailoverQueryTimeNanos, 1, lastFailoverQueryTimeNanos);
        this.machineState = new MachineState(failover, (Load) null);

        this.history = null;
        this.historyFailover = null;
        this.loadFactory = null;
        this.setPeriod(false, -1);
        this.setPeriod(true, -1);
        this.setCycleModuloPeriod(false, -1);
        this.setCycleModuloPeriod(true, -1);
        this.currentToUnusedRatio = -1;
        this.backOffMultiplier = -1;
        this.backOffMaxRatio = -1;
        this.mainDbName = null;
        this.failoverDbName = null;

        assert !isLocalState() && !isLocalStateCombinedWithRemoteOne() && isRemoteState();
    }

    private boolean isLocalState() {
        return loadFactory != null && getPeriod(false) != -1;
    }

    private boolean isLocalStateCombinedWithRemoteOne() {
        return loadFactory != null && getPeriod(false) == -1;
    }

    private boolean isRemoteState() {
        return loadFactory == null && getPeriod(false) == -1;
    }

    @Override
    public State clone() {
        try {
            State copy = (State) super.clone();

            // Now elements that need deep copying (arrays and mutable non-primitive types ()):
            if (history != null) {
                copy.history = history.clone();
            }
            if (historyFailover != null) {
                copy.historyFailover = historyFailover.clone();
            }
            copy.period = new int[]{period[0], period[1]};
            copy.cycleModuloPeriod = new int[]{cycleModuloPeriod[0], cycleModuloPeriod[1]};
            copy.stateAfterLastMeasurement = new StateAfterMeasurement[]{
                    stateAfterLastMeasurement[0], stateAfterLastMeasurement[1]};
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

    public String getRepoId() {
        return repoId;
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
        if (failoverDbName != null ? !failoverDbName.equals(state.failoverDbName) : state.failoverDbName != null)
            return false;
        if (history != null ? !history.equals(state.history) : state.history != null) return false;
        if (historyFailover != null ? !historyFailover.equals(state.historyFailover) : state.historyFailover != null)
            return false;
        if (host != null ? !host.equals(state.host) : state.host != null) return false;
        if (loadAfterLastMeasurement != null ?
                !loadAfterLastMeasurement.equals(state.loadAfterLastMeasurement) :
                state.loadAfterLastMeasurement != null)
            return false;
        if (loadFactory != null ? !loadFactory.equals(state.loadFactory) : state.loadFactory != null) return false;
        if (machineState != null ? !machineState.equals(state.machineState) : state.machineState != null) return false;
        if (mainDbName != null ? !mainDbName.equals(state.mainDbName) : state.mainDbName != null) return false;
        if (!Arrays.equals(period, state.period)) return false;
        if (repoId != null ? !repoId.equals(state.repoId) : state.repoId != null) return false;
        if (!Arrays.equals(stateAfterLastMeasurement, state.stateAfterLastMeasurement)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (modifyTimeMillis ^ (modifyTimeMillis >>> 32));
        result = 31 * result + (avg != null ? avg.hashCode() : 0);
        result = 31 * result + (avgFailover != null ? avgFailover.hashCode() : 0);
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (repoId != null ? repoId.hashCode() : 0);
        result = 31 * result + (history != null ? history.hashCode() : 0);
        result = 31 * result + (historyFailover != null ? historyFailover.hashCode() : 0);
        result = 31 * result + (machineState != null ? machineState.hashCode() : 0);
        result = 31 * result + (loadFactory != null ? loadFactory.hashCode() : 0);
        result = 31 * result + (period != null ? Arrays.hashCode(period) : 0);
        result = 31 * result + (cycleModuloPeriod != null ? Arrays.hashCode(cycleModuloPeriod) : 0);
        result = 31 * result + currentToUnusedRatio;
        result = 31 * result + backOffMultiplier;
        result = 31 * result + backOffMaxRatio;
        result = 31 * result + (mainDbName != null ? mainDbName.hashCode() : 0);
        result = 31 * result + (failoverDbName != null ? failoverDbName.hashCode() : 0);
        result = 31 * result + (loadAfterLastMeasurement != null ? loadAfterLastMeasurement.hashCode() : 0);
        result = 31 * result + (stateAfterLastMeasurement != null ? Arrays.hashCode(stateAfterLastMeasurement) : 0);
        return result;
    }

    // toString() ALMOST auto-generated using all fields. ALMOST because all arrays are written using Arrays.toString():
    @Override
    public String toString() {
        return "State{" +
                "modifyTimeMillis=" + modifyTimeMillis +
                ", avg=" + avg +
                ", avgFailover=" + avgFailover +
                ", host='" + host + '\'' +
                ", repoId='" + repoId + '\'' +
                ", history=" + history +
                ", historyFailover=" + historyFailover +
                ", machineState=" + machineState +
                ", loadFactory=" + loadFactory +
                ", period=" + Arrays.toString(period) +
                ", cycleModuloPeriod=" + Arrays.toString(cycleModuloPeriod) +
                ", currentToUnusedRatio=" + currentToUnusedRatio +
                ", backOffMultiplier=" + backOffMultiplier +
                ", backOffMaxRatio=" + backOffMaxRatio +
                ", mainDbName='" + mainDbName + '\'' +
                ", failoverDbName='" + failoverDbName + '\'' +
                ", loadAfterLastMeasurement=" + loadAfterLastMeasurement +
                ", stateAfterLastMeasurement=" + Arrays.toString(stateAfterLastMeasurement) +
                '}';
    }

    /**
     * Update this state with new sql times. If update fails for some reason then all changes made to this state are
     * reverted and this state is returned. If update succeeds then this method returns the clone of this state as it
     * was before this method was invoked.
     *
     *
     * @param logPrefix
     * @param mainDbStmtExecTimeNanos
     * @param failoverDbStmtExecTimeNanos
     * @return clone of this state as it was before this method was invoked or this state if
     */
    public State updateLocalStateWithNewExecTimes(MonitorRunLogPrefix logPrefix,
                                                  long mainDbStmtExecTimeNanos,
                                                  long failoverDbStmtExecTimeNanos) {
        State copy = clone();

        try {
            copy.doUpdateLocalStateWithNewExecTimes(
                    logPrefix,
                    mainDbStmtExecTimeNanos,
                    failoverDbStmtExecTimeNanos
            );
        } catch (RuntimeException e) {
            logger.error(logPrefix + "exception (shown below) while updating state - " +
                    "reverted all changes made during this operation", e);
            return this.clone();
        }

        return updateLocalStateWithNewExecTimes(copy);
    }

    public void doUpdateLocalStateWithNewExecTimes(MonitorRunLogPrefix logPrefix,
                                                   long mainDbStmtExecTimeNanos,
                                                   long failoverDbStmtExecTimeNanos) {
        modifyTimeMillis = System.currentTimeMillis();

        if (!isLocalState()) {
            throw new IllegalStateException("this.updateLocalStateWithNewExecTimes method can be " +
                    "invoked only when this.isLocalState() is true but this is not the case: this=" + this);
        }

        if (validateSqlTime(false, mainDbStmtExecTimeNanos)) {
            avg = history.updateAverage(mainDbStmtExecTimeNanos);
        }
        if (validateSqlTime(true, failoverDbStmtExecTimeNanos)) {
            avgFailover = historyFailover.updateAverage(failoverDbStmtExecTimeNanos);
        }

        loadAfterLastMeasurement = loadFactory.getLoad(avg.getValue(), avgFailover.getValue());
        MachineState oldMachineState = machineState;
        machineState = stateMachine.transition(oldMachineState, loadAfterLastMeasurement);

        updatedCycleAndPeriodOfLocalState(logPrefix, mainDbStmtExecTimeNanos, oldMachineState, false);
        updatedCycleAndPeriodOfLocalState(logPrefix, failoverDbStmtExecTimeNanos, oldMachineState, true);
    }

    private State updateLocalStateWithNewExecTimes(State s) {
        State old = clone();

        modifyTimeMillis         = s.getModifyTimeMillis();
        avg                      = s.getAvg();
        avgFailover              = s.getAvgFailover();
        loadAfterLastMeasurement = s.getLoadAfterLastMeasurement();
        machineState             = s.getMachineState();
        history                  = s.getHistory();
        historyFailover          = s.getHistoryFailover();

        setStateAfterLastMeasurement(false, s.getStateAfterLastMeasurement(false));
        setStateAfterLastMeasurement(true, s.getStateAfterLastMeasurement(true));

        setCycleModuloPeriod(false, s.getCycleModuloPeriod(false));
        setCycleModuloPeriod(true,  s.getCycleModuloPeriod(true));

        setPeriod(false, s.getPeriod(false));
        setPeriod(true, s.getPeriod(true));

        return old;
    }

    private boolean validateSqlTime(boolean failover, long sqlTimeNanos) {
        if (sqlTimeNanos != notMeasuredInThisCycle) {
            Utils.assertNonNegative(sqlTimeNanos, "sqlTimeNanos");
            Utils.assertSame(0, getCycleModuloPeriod(failover), "given sql time ("
                    + ExceptionEnum.erroneousValuesAsStr(sqlTimeNanos) + ") indicates that the "
                    + (failover ? "failover" : "main")
                    + " db load was measured; this should not be the case as its cycleModuloPeriod != 0");
            return true;
        } else {
            if (getCycleModuloPeriod(failover) <= 0) {
                throw new IllegalStateException("sql time was not measured in this cycle for "
                        + (failover ? "failover" : "main")
                        + " db; hence cycleModuloPeriod for this db should be greater than zero but it isn't: "
                        + getCycleModuloPeriod(failover));
            }
            return false;
        }
    }

    private void updatedCycleAndPeriodOfLocalState(MonitorRunLogPrefix logPrefix,
                                                   long sqlTimeNanos,
                                                   MachineState oldMachineState,
                                                   boolean failover) {
        logPrefix = logPrefix.append((failover ? failoverDbName : mainDbName) + ": ");
        if (sqlTimeNanos != notMeasuredInThisCycle) {
            if (okDbStillNotUsed(
                    failover, oldMachineState.isFailoverActive(), oldMachineState.getLoad().getLoadLevel(failover))) {
                keepDecreasedLoadOfUnusedOkDb(logPrefix, failover);
            } else if (notOkDb(failover)) {
                decreaseLoadOfNotOkDb(logPrefix, failover);
            } else if (dbNotUsed(failover)) {
                assertOkDb(failover);
                decreaseLoadOfOkDbThatBecameUnused(logPrefix, failover);
            } else {
                assertOkDb(failover);
                keepNormalLoadOfUsedOkDb(logPrefix, failover);
            }
        } else {
            increaseCycleModuloPeriod(failover);
        }
    }

    private void assertOkDb(boolean failover) {
        if (machineState.getLoad().getLoadLevel(failover) != low
                && machineState.getLoad().getLoadLevel(failover) != medium) {
            throw new IllegalStateException((failover ? "failover" : "main") + " db should not have problems");
        }
    }

    private void keepDecreasedLoadOfUnusedOkDb(MonitorRunLogPrefix logPrefix, boolean failover) {
        if (getPeriod(failover) != currentToUnusedRatio) {
            throw new IllegalStateException("period != currentToUnusedRatio (" + currentToUnusedRatio
                    + ") for still not used " + (failover ? "failover" : "main") + " db");
        }
        logger.info(logPrefix + "db still unused: period=currentToUnusedRatio=" + currentToUnusedRatio);
        increaseCycleModuloPeriod(failover);
        setStateAfterLastMeasurement(failover, StateAfterMeasurement.okDbStillNotUsed);
    }

    private void decreaseLoadOfNotOkDb(MonitorRunLogPrefix logPrefix, boolean failover) {
        int oldPeriod = getPeriod(failover);
        setPeriod(failover, oldPeriod * backOffMultiplier);
        if (getPeriod(failover) > backOffMaxRatio) {
            setPeriod(failover, backOffMaxRatio);
        }
        logger.info(logPrefix + "load level at least high: increasing period to decrease load: old period=" +
                oldPeriod + ", new period=" + getPeriod(failover));
        increaseCycleModuloPeriod(failover);
        setStateAfterLastMeasurement(failover, StateAfterMeasurement.notOkDb);
    }

    private void decreaseLoadOfOkDbThatBecameUnused(MonitorRunLogPrefix logPrefix, boolean failover) {
        if (getPeriod(failover) != 1 || getCycleModuloPeriod(failover) != 0) {
            throw new IllegalStateException("decreaseLoadOfOkDbThatBecameUnused method used but period"
                    + " != 1 (" + getPeriod(failover) + ") or cycleModuloPeriod"
                    + " != 0 (" + getCycleModuloPeriod(failover) + ") for " + getDbName(failover));
        }
        logger.info(logPrefix + "db became unused: old period=1, new period=currentToUnusedRatio="
                + currentToUnusedRatio);
        setPeriod(failover, currentToUnusedRatio);
        increaseCycleModuloPeriod(failover);
        setStateAfterLastMeasurement(failover, StateAfterMeasurement.okDbBecameUnused);
    }

    private void keepNormalLoadOfUsedOkDb(MonitorRunLogPrefix logPrefix, boolean failover) {
        if (getPeriod(failover) > 1) {
            logger.info(logPrefix + "back to period=1 (old period=" + getPeriod(failover)
                    + ") for " + getDbName(failover));
        }
        setPeriod(failover, 1);
        setCycleModuloPeriod(failover, 0);
        setStateAfterLastMeasurement(failover, StateAfterMeasurement.okDbUsed);
    }

    private void increaseCycleModuloPeriod(boolean failover) {
        int i = getCycleModuloPeriod(failover) + 1;
        if (i < getPeriod(failover)) {
            setCycleModuloPeriod(failover, i);
        } else {
            setCycleModuloPeriod(failover, 0);
        }
    }

    private boolean okDbStillNotUsed(boolean failover, boolean wasFailoverActive, LoadLevel old) {
        LoadLevel cur = machineState.getLoad().getLoadLevel(failover);
        if (failover) {
            return !wasFailoverActive && !machineState.isFailoverActive()
                    && (old == low || old == medium)
                    && (cur == low || cur == medium);
        } else {
            return wasFailoverActive && machineState.isFailoverActive()
                    && (old == low || old == medium)
                    && (cur == low || cur == medium);
        }
    }

    private boolean dbNotUsed(boolean failover) {
        if (failover) {
            return !machineState.isFailoverActive() &&
                    (machineState.getLoad().getFailoverDb() == low || machineState.getLoad().getFailoverDb() == medium);
        } else {
            return machineState.isFailoverActive() &&
                    (machineState.getLoad().getMainDb() == low || machineState.getLoad().getMainDb() == medium);
        }
    }

    private boolean notOkDb(boolean failover) {
        LoadLevel l = machineState.getLoad().getLoadLevel(failover);
        return l == high || l == exceptionWhileMeasuring;
    }

    public State copyFrom(MonitorRunLogPrefix logPrefixIfLocalState, State localOrRemote) {
        if (isRemoteState()) {
            throw new IllegalStateException("this state must not be a remote one");
        }
        if (localOrRemote.isLocalStateCombinedWithRemoteOne()) {
            throw new IllegalStateException("state to copy from (localOrRemote) must not be a copy of a remote state");
        }

        State oldState = clone();

        this.modifyTimeMillis = localOrRemote.getModifyTimeMillis();
        this.loadAfterLastMeasurement = localOrRemote.loadAfterLastMeasurement;
        this.avg = localOrRemote.avg;
        this.avgFailover = localOrRemote.avgFailover;
        this.host = localOrRemote.host;
        this.repoId = localOrRemote.repoId;
        this.machineState = localOrRemote.machineState;
        this.setPeriod(false, localOrRemote.getPeriod(false));
        this.setPeriod(true, localOrRemote.getPeriod(true));
        this.setCycleModuloPeriod(false, localOrRemote.getCycleModuloPeriod(false));
        this.setCycleModuloPeriod(true, localOrRemote.getCycleModuloPeriod(true));

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

        return oldState;
    }

    private void checkConfigsIdentical(MonitorRunLogPrefix logPrefix, State fullState) {
        if (loadFactory.getFailoverThresholdNanos() != fullState.loadFactory.getFailoverThresholdNanos()
                || loadFactory.getFailbackThresholdNanos() != fullState.loadFactory.getFailbackThresholdNanos()
                || currentToUnusedRatio != fullState.currentToUnusedRatio
                || backOffMultiplier != fullState.backOffMultiplier
                || backOffMaxRatio != fullState.backOffMaxRatio) {
            logger.warn(logPrefix + "config difference detected; local state:\n" +
                    this + "\nremote state:\n" + fullState);
        }

    }

    public boolean sqlTimeIsMeasuredInThisCycle(boolean failover) {
        return sqlTimeIsMeasuredInThisCycle(null, failover);
    }

    public boolean sqlTimeIsMeasuredInThisCycle(MonitorRunLogPrefix logPrefix, boolean failover) {
        boolean isMeasured = getCycleModuloPeriod(failover) == 0;
        if (!isMeasured) {
            if (logPrefix != null) {
                logger.debug(logPrefix + "not measured in this cycle (cycle=" + getCycleModuloPeriod(failover)
                        + ", period=" + getPeriod(failover) + ", " + getStateAfterLastMeasurement(failover) + ")");
            }
        }
        return isMeasured;
    }

    public int getPeriod(boolean failover) {
        return period[index(failover)];
    }

    private void setPeriod(boolean failover, int i) {
        period[index(failover)] = i;
    }

    public int getCycleModuloPeriod(boolean failover) {
        return cycleModuloPeriod[index(failover)];
    }

    private void setCycleModuloPeriod(boolean failover, int i) {
        cycleModuloPeriod[index(failover)] = i;
    }

    private int index(boolean failover) {
        return failover ? failoverIndex : mainIndex;
    }

    private String getDbName(boolean failover) {
        return (failover ? failoverDbName : mainDbName);
    }

    public Load getLoadAfterLastMeasurement() {
        return loadAfterLastMeasurement;
    }

    private SqlTimeHistory getHistory() {
        return history;
    }

    private SqlTimeHistory getHistoryFailover() {
        return historyFailover;
    }

    private StateAfterMeasurement getStateAfterLastMeasurement(boolean failover) {
        return stateAfterLastMeasurement[index(failover)];
    }

    private void setStateAfterLastMeasurement(boolean failover, StateAfterMeasurement s) {
        stateAfterLastMeasurement[index(failover)] = s;
    }

    public String getDesc(boolean failover) {
        if (getStateAfterLastMeasurement(failover) != null) {
            return getStateAfterLastMeasurement(failover).toString();
        } else {
            return "db load not measured yet";
        }
    }
}
