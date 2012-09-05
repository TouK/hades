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
import pl.touk.hades.ConnectionListener;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;

import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;

import static pl.touk.hades.Utils.indent;

/**
* Monitor that activates failover when the main data source
* is overloaded in comparison to the failover data source.
*
* @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
*/
public class SqlTimeBasedMonitorImpl implements SqlTimeBasedMonitor {

    static private final Logger logger = LoggerFactory.getLogger(SqlTimeBasedMonitorImpl.class);

    private final Hades<SqlTimeBasedMonitorImpl> hades;
    private final SqlTimeCalculator calc;

    private final State state;

    private final AtomicReference<ConnectionListener> connectionListener = new AtomicReference<ConnectionListener>();

    public SqlTimeBasedMonitorImpl(Hades<SqlTimeBasedMonitorImpl> hades,
                                   int sqlTimeTriggeringFailoverMillis,
                                   int sqlTimeTriggeringFailbackMillis,
                                   int sqlTimesIncludedInAverage,
                                   boolean exceptionsIgnoredAfterRecovery,
                                   boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                                   SqlTimeCalculator calc,
                                   int currentToUnusedRatio,
                                   int backOffMultiplier,
                                   int backOffMaxRatio,
                                   String host) throws UnknownHostException {

        Utils.assertNotNull(hades, "hades");
        Utils.assertNonNegative(sqlTimeTriggeringFailoverMillis, "sqlTimeTriggeringFailoverMillis");
        Utils.assertNonNegative(sqlTimeTriggeringFailbackMillis, "sqlTimeTriggeringFailbackMillis");
        Utils.assertPositive(sqlTimesIncludedInAverage, "sqlTimesIncludedInAverage");
        Utils.assertNotNull(calc, "calc");

        this.hades = hades;
        this.calc = calc;

        this.state = new State(new SqlTimeBasedLoadFactory(Utils.millisToNanos(sqlTimeTriggeringFailoverMillis),
                                                           Utils.millisToNanos(sqlTimeTriggeringFailbackMillis)),
                host,
                sqlTimesIncludedInAverage,
                exceptionsIgnoredAfterRecovery,
                recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                currentToUnusedRatio,
                backOffMultiplier,
                backOffMaxRatio,
                hades.getDsName(false, true),
                hades.getDsName(true, true));
    }

    public void init(ConnectionListener listener) {
        hades.init(this);
        connectionListener.set(listener);
    }

    State run(String logPrefix) {
        logger.debug(logPrefix + "run started");
        try {
            long[] times = calc.calculateMainAndFailoverSqlTimesNanos(indent(logPrefix), getState());
            return updateState(indent(logPrefix), times[0], times[1]);
        } catch (InterruptedException e) {
            logger.info(indent(logPrefix) + "measuring interrupted");
            Thread.currentThread().interrupt();
            return getState();
        }
    }

    public void connectionRequestedFromHades(boolean success, boolean failover, long timeNanos) {
        if (connectionListener.get() != null) {
            connectionListener.get().connectionRequestedFromHades(success, failover, timeNanos);
        }
    }

    private State updateState(String logPrefix, long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos) {
        State oldState;
        State newState;
        synchronized (state) {
            oldState = state.clone();
            state.updateLocalStateWithNewExecTimes(indent(logPrefix), mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos, null);
            newState = state.clone();
        }
        if (newState.getMachineState().isFailoverActive() != oldState.getMachineState().isFailoverActive()) {
            warnAboutFailbackOrFailover(logPrefix, oldState, newState);
        } else {
            infoAboutPreservingFailoverOrFailback(logPrefix, oldState, newState);
        }
        return newState;
    }

    public long getLastQueryTimeMillis(boolean main) {
        return Utils.nanosToMillis(getLastQueryTimeNanos(main));
    }

    public long getLastQueryTimeNanos(boolean main) {
        long nanos;
        synchronized (state) {
            nanos = (main ? state.getAvg() : state.getAvgFailover()).getLast();
        }
        return nanos;
    }

    private void warnAboutFailbackOrFailover(String curRunLogPrefix, State oldState, State newState) {
        logger.warn(curRunLogPrefix + calc.getLog(oldState, newState));
    }

    private void infoAboutPreservingFailoverOrFailback(String curRunLogPrefix, State oldState, State newState) {
        logger.info(curRunLogPrefix + calc.getLog(oldState, newState));
    }

    public Load getLoad() {
        synchronized (state) {
            return state.getMachineState().getLoad();
        }
    }

    public String getLoadLog() {
        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
        synchronized (state) {
            boolean failoverEffectivelyActive = failoverDataSourcePinned != null ? failoverDataSourcePinned : state.getMachineState().isFailoverActive();
            return Utils.nanosToMillisAsStr(state.getAvg().getValue()) + " (" + state.getMachineState().getLoad().getMainDb() + ") - " + hades.getMainDsName() + ", " +
                   Utils.nanosToMillisAsStr(state.getAvgFailover().getValue()) + " (" + state.getMachineState().getLoad().getFailoverDb() + ") - " + hades.getFailoverDsName() +
                   ", using " + (failoverEffectivelyActive ? hades.getFailoverDsName() : hades.getMainDsName()) + (failoverDataSourcePinned != null ? " (PINNED)" : "");
        }
    }

    public boolean isFailoverActive() {
        synchronized (state) {
            return state.getMachineState().isFailoverActive();
        }
    }

    State setState(String logPrefixIfFullState, State newState) {
        State oldState;
        synchronized (state) {
            oldState = state.clone();
            state.copyFrom(logPrefixIfFullState, newState);
        }
        return oldState;
    }

    public State getState() {
        synchronized (state) {
            return state.clone();
        }
    }

    public Hades getHades() {
        return hades;
    }
}
