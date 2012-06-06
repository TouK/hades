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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.text.DecimalFormat;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;

import pl.touk.hades.*;
import pl.touk.hades.load.statemachine.MachineState;
import pl.touk.hades.load.*;

/**
 * A failover activator that activates failover when the main data source
 * is overloaded in comparison to the failover data source. The exact algorithm used is described in {@link #run()}.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class SqlTimeBasedTriggerImpl extends TimerTask implements SqlTimeBasedTrigger {

    static private final Logger logger = LoggerFactory.getLogger(SqlTimeBasedTriggerImpl.class);

    private long sqlTimeTriggeringFailoverMillis = 100;
    private long sqlTimeTriggeringFailoverNanos = sqlTimeTriggeringFailoverMillis * Hades.nanosInMillisecond;
    private int sqlTimeTriggeringFailbackMillis = 50;
    private long sqlTimeTriggeringFailbackNanos = sqlTimeTriggeringFailbackMillis * Hades.nanosInMillisecond;

    private int sqlTimesIncludedInAverage = 1;
    private boolean exceptionsIgnoredAfterRecovery = true;
    private boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery = true;

    private boolean includeDebuggingInfoInEveryLog = false;

    private volatile long endOfLastRunMethod = 0;

    public static final DecimalFormat decimalFormat = new DecimalFormat("#0.000 ms");

//    private SqlTimeBasedHadesLoadFactory loadFactory;
//
//    private final Machine stateMachine = Machine.createStateMachine();

    // Guards everything related to the current state, i.e. current load, current state, averages, statement execution time histories.
    private final Object stateGuard = new Object();
    private State state = new State();
//    private HadesLoad currentLoad;
//    private Average mainDbAverageNanos;
//    private Average failoverDbAverageNanos;
//    private MachineState currentMachineState = Machine.initialState;
//    private SqlTimeHistory sqlMainDbTimeHistory = null;
//    private SqlTimeHistory sqlFailoverDbTimeHistory = null;

    private Hades hades;
    private SqlTimeCalculator sqlTimeCalculator;

    private DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss,SSS");

    private String cron;
    private Scheduler scheduler;
    private String schedulerName;
    private String schedulerInstanceId;
    private static final List<SqlTimeBasedTriggerImpl> sqlTimeBasedTriggers = new ArrayList<SqlTimeBasedTriggerImpl>();
    private final int sqlTimeBasedTriggersIndex;
    private static final String sqlTimeBasedTriggersIndexKey = "sqlTimeBasedTriggersIndex";
    private static final String stateKey = "sqlTimeBasedTriggerState";
    private String hadesQuartzGroup;
    private String hadesQuartzJob;
    private String hadesQuartzTrigger;
    private static final String hadesQuartzGroupPrefix = "HADES_GROUP_";
    private static final String hadesQuartzJobPrefix = "HADES_JOB_";
    private static final String hadesQuartzTriggerPrefix = "HADES_TRIGGER_";

    public SqlTimeBasedTriggerImpl() {
        synchronized (sqlTimeBasedTriggers) {
            sqlTimeBasedTriggers.add(this);
            sqlTimeBasedTriggersIndex = sqlTimeBasedTriggers.size() - 1;
        }
    }

    public void run() {
        run(null);
    }

    /**
     * Calculates current load for the controlled {@link pl.touk.hades.Hades} and activates failover or failback if needed.
     * <p/>
     * This method should be invoked periodically. In every invocation the current load is got from a
     * load factory created with constructor
     * {@link SqlTimeBasedHadesLoadFactory#SqlTimeBasedHadesLoadFactory(long, long) LoadFactory(stmtExecTimeLimitTriggeringFailoverMillis, statementExecutionTimeLimitTriggeringFailbackMillis)},
     * where values used as parameters are those set in {@link #setSqlTimeTriggeringFailoverMillis(int)}
     * and {@link #setSqlTimeTriggeringFailbackMillis(int)}. The current load is produced through
     * invocation of
     * {@link SqlTimeBasedHadesLoadFactory#getLoad(long, long) getLoad(averageMainDsExecTime, averageFailoverDsExecTime)} on the above
     * factory,
     * where averages used as parameters are calculated for both data sources for last <i>N</i>
     * (which can be set in {@link #setSqlTimesIncludedInAverage(int)}) execution times of an sql
     * statement. During each invocation of this method the sql
     * statement is executed on the main data source and the failover data source and the execution times are measured.
     * These two execution times are included in above averages passed to the factory.
     * <p/>
     * When current load is known a decision is made whether failover or failback should be activated as follows.
     * Failover is activated when main database load becomes high and the failover database works and its load is
     * smaller. Failback is activated when main database load returns to low or becomes smaller than the high failover
     * database load.
     * @param map quartz job data map if the current invoction originates from quartz
     */
    public void run(JobDataMap map) {
        String logPrefix = "[" + hades.toString() + ", ts=" + System.currentTimeMillis() + "] " + getSchedulerInfo();

        logger.info(logPrefix + "start of method measuring load levels");

        // TODO: kazdy hades powinien zarejestrowac listenera w quartzu, by quartz go powiadomił o wykonaniu joba
        // pomiaru przez innego hadesa w klastrze hadesowym. Wtedy poniższe bedzie niepotrzebne:
        restoreStateFromJobDataMap(logPrefix, map);

        if (!lastExecutionDelayedThisExecution(logPrefix)) {
            try {
                long mainDbStmtExecTimeNanos = sqlTimeCalculator.calculateSqlTimeNanos(hades, logPrefix, false);
                long failoverDbStmtExecTimeNanos = sqlTimeCalculator.calculateSqlTimeNanos(hades, logPrefix, true);
                updateState(logPrefix, mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos);

                ensureThatExecutionsDelayedByThisExecutionAreAbandoned();
            } catch (InterruptedException e) {
                logger.info(logPrefix + "measuring interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }

    private String getSchedulerInfo() {
        if (includeDebuggingInfoInEveryLog) {
            return scheduler != null ? "[" + schedulerName + ", " + schedulerInstanceId + ", " + hades.getName() + "] " : "[null scheduler, " + hades.getName() + "] ";
        } else {
            return "";
        }
    }

    private void updateState(String curRunLogPrefix, long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos) {
        State oldState;
        State newState;
        synchronized (stateGuard) {
            oldState = state.clone();
            state.update(mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos);
            newState = state.clone();
//            updateAveragesAndCurrentLoad(mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos);
//            makeStateTransitionCorrespondingToCurrentLoad(curRunLogPrefix);
        }
        if (newState.getMachineState().isFailoverActive() != oldState.getMachineState().isFailoverActive()) {
            warnAboutFailbackOrFailover(curRunLogPrefix, oldState, newState);
        } else {
            infoAboutPreservingFailoverOrFailback(curRunLogPrefix, oldState, newState);
        }
    }

    public long getLastFailoverQueryTimeMillis() {
        synchronized (stateGuard) {
            return state.getAvgFailover().getLast() / Hades.nanosInMillisecond;
        }
    }

//    private void makeStateTransitionCorrespondingToCurrentLoad(String curRunLogPrefix) {
//        MachineState oldMachineState = state.getMachineState();
////        MachineState oldMachineState = currentMachineState;
//        state.setMachineState(stateMachine.transition(oldMachineState, state.getLoad()));
////        currentMachineState = stateMachine.transition(oldMachineState, currentLoad);
//        if (state.getMachineState().isFailoverActive() != oldMachineState.isFailoverActive()) {
////        if (currentMachineState.isFailoverActive() != oldMachineState.isFailoverActive()) {
//            warnAboutFailbackOrFailover(curRunLogPrefix, oldMachineState, state.getMachineState().isFailoverActive());
////            warnAboutFailbackOrFailover(curRunLogPrefix, oldMachineState, currentMachineState.isFailoverActive());
//        } else {
//            infoAboutPreservingFailoverOrFailback(curRunLogPrefix, oldMachineState);
//        }
//    }

//    private void updateAveragesAndCurrentLoad(long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos) {
//        state.update(mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos);
////        mainDbAverageNanos = sqlMainDbTimeHistory.updateAverage(mainDbStmtExecTimeNanos);
////        failoverDbAverageNanos = sqlFailoverDbTimeHistory.updateAverage(failoverDbStmtExecTimeNanos);
////        currentLoad = loadFactory.getLoad(mainDbAverageNanos.getValue(), failoverDbAverageNanos.getValue());
//    }

    private void initStmtExecTimeHistories() {
        state = new State(new SqlTimeBasedHadesLoadFactory(sqlTimeTriggeringFailoverNanos, sqlTimeTriggeringFailbackNanos));
        state.createHistories(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
//        sqlMainDbTimeHistory =     new SqlTimeHistory(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
//        sqlFailoverDbTimeHistory = new SqlTimeHistory(sqlTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
//        loadFactory = new SqlTimeBasedHadesLoadFactory(sqlTimeTriggeringFailoverNanos, sqlTimeTriggeringFailbackNanos);
    }

//    private String createTransitionDesc(MachineState oldState) {
//        boolean failoverOrFailback = currentMachineState.isFailoverActive() != oldState.isFailoverActive();
//        String dbSwitchType;
//        if (failoverOrFailback) {
//            dbSwitchType = currentMachineState.isFailoverActive() ? "activating failover (" + hades.getMainDataSourceName() + " -> " + hades.getFailoverDataSourceName() + ")" : "activating failback (" + hades.getFailoverDataSourceName() + " -> " + hades.getMainDataSourceName() + ")";
//        } else {
//            if (currentMachineState.isFailoverActive()) {
//                dbSwitchType = "failover remains active (keep using " + hades.getFailoverDataSourceName() + ")";
//            } else {
//                dbSwitchType = "failover remains inactive (keep using " + hades.getMainDataSourceName() + ")";
//            }
//        }
//        return dbSwitchType;
//    }

    private String createTransitionDesc(State oldState, State newState) {
        boolean failoverOrFailback = newState.getMachineState().isFailoverActive() != oldState.getMachineState().isFailoverActive();
        String dbSwitchType;
        if (failoverOrFailback) {
            dbSwitchType = newState.getMachineState().isFailoverActive() ? "activating failover (" + hades.getMainDataSourceName() + " -> " + hades.getFailoverDataSourceName() + ")" : "activating failback (" + hades.getFailoverDataSourceName() + " -> " + hades.getMainDataSourceName() + ")";
        } else {
            if (newState.getMachineState().isFailoverActive()) {
                dbSwitchType = "failover remains active (keep using " + hades.getFailoverDataSourceName() + ")";
            } else {
                dbSwitchType = "failover remains inactive (keep using " + hades.getMainDataSourceName() + ")";
            }
        }
        return dbSwitchType;
    }

    private boolean lastExecutionDelayedThisExecution(String curRunLogPrefix) {
        long scheduledExecutionTime = scheduledExecutionTime();
        if (scheduledExecutionTime > 0) {
            long endOfLastRunMethodCopy = endOfLastRunMethod;
            if (scheduledExecutionTime < endOfLastRunMethodCopy) {
                logger.warn(curRunLogPrefix + "last execution ended at " + df.format(new Date(endOfLastRunMethodCopy)) + " which is after this execution's scheduled time: " + df.format(new Date(scheduledExecutionTime)) + "; therefore this execution is abandoned");
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private void ensureThatExecutionsDelayedByThisExecutionAreAbandoned() {
        endOfLastRunMethod = System.currentTimeMillis();
    }

//    private void warnAboutFailbackOrFailover(String curRunLogPrefix, MachineState oldState, boolean failover) {
//        if (failover) {
//            logger.warn(curRunLogPrefix + getLog(oldState));
//        } else {
//            logger.warn(curRunLogPrefix + getLog(oldState));
//        }
//    }

    private void warnAboutFailbackOrFailover(String curRunLogPrefix, State oldState, State newState) {
        logger.warn(curRunLogPrefix + getLog(oldState, newState));
    }

//    private void infoAboutPreservingFailoverOrFailback(String curRunLogPrefix, MachineState oldState) {
//        logger.info(curRunLogPrefix + getLog(oldState));
//    }

    private void infoAboutPreservingFailoverOrFailback(String curRunLogPrefix, State oldState, State newState) {
        logger.info(curRunLogPrefix + getLog(oldState, newState));
    }

//    private String getLog(MachineState oldState) {
//        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
//        return "average execution time for '" + sqlTimeCalculator.getSql() + "' for last " + mainDbAverageNanos.getItemsCountIncludedInAverage() + " execution(s): "
//                + nanosToMillisHumanReadable(mainDbAverageNanos.getValue()) + " (last: " + nanosToMillisHumanReadable(mainDbAverageNanos.getLast()) + ") - " + hades.getMainDataSourceName() + ", "
//                + nanosToMillisHumanReadable(failoverDbAverageNanos.getValue()) + " (last: " + nanosToMillisHumanReadable(failoverDbAverageNanos.getLast()) + ") - " + hades.getFailoverDataSourceName()
//                + "; load levels derived from average execution times: "
//                + getLoadLevels() + "; "
//                + createTransitionDesc(oldState)
//                + (failoverDataSourcePinned != null ? "; above calculations currently have no effect because " + (failoverDataSourcePinned ? "failover data source (" + hades.getFailoverDataSourceName() + ")" : "main data source (" + hades.getMainDataSourceName() + ")") + " is pinned and therefore used" : "");
//    }

    private String getLog(State oldState, State newState) {
        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
        return "average execution time for '" + sqlTimeCalculator.getSql() + "' for last " + newState.getAvg().getItemsCountIncludedInAverage() + " execution(s): "
                + nanosToMillisHumanReadable(newState.getAvg().getValue()) + " (last: " + nanosToMillisHumanReadable(newState.getAvg().getLast()) + ") - " + hades.getMainDataSourceName() + ", "
                + nanosToMillisHumanReadable(newState.getAvgFailover().getValue()) + " (last: " + nanosToMillisHumanReadable(newState.getAvgFailover().getLast()) + ") - " + hades.getFailoverDataSourceName()
                + "; load levels derived from average execution times: "
                + getLoadLevels(newState) + "; "
                + createTransitionDesc(oldState, newState)
                + (failoverDataSourcePinned != null ? "; above calculations currently have no effect because " + (failoverDataSourcePinned ? "failover data source (" + hades.getFailoverDataSourceName() + ")" : "main data source (" + hades.getMainDataSourceName() + ")") + " is pinned and therefore used" : "");
    }

//    private String getLoadLevels() {
//        HadesLoad load = currentMachineState.getLoad();
//        return currentMachineState.getLoad().getMainDb() + " - " + hades.getMainDataSourceName() + ", "
//                + currentMachineState.getLoad().getFailoverDb() + " - " + hades.getFailoverDataSourceName()
//                + (load.isMainDbLoadHigher() != null ?
//                " (" + hades.getMainDataSourceName() + " load level is" + (load.isMainDbLoadHigher() ? "" : " not") + " higher)"
//                : "");
//    }

    private String getLoadLevels(State state) {
        HadesLoad load = state.getMachineState().getLoad();
        return load.getMainDb()     + " - " + hades.getMainDataSourceName() + ", " +
               load.getFailoverDb() + " - " + hades.getFailoverDataSourceName() +
               (load.isMainDbLoadHigher() != null ? " (" + hades.getMainDataSourceName() + " load level is" + (load.isMainDbLoadHigher() ? "" : " not") + " higher)" : "");
    }

    public static String nanosToMillisHumanReadable(long l) {
        if (l < Long.MAX_VALUE) {
            return decimalFormat.format(((double) l) / Hades.nanosInMillisecond);
        } else {
            return "<N/A because of exception(s) - assuming infinity>";
        }
    }

    public String getLoadLog() {
        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
        synchronized (stateGuard) {
            boolean failoverEffectivelyActive = failoverDataSourcePinned != null ? failoverDataSourcePinned : state.getMachineState().isFailoverActive();
            return nanosToMillisHumanReadable(state.getAvg().getValue()) + " (" + state.getMachineState().getLoad().getMainDb() + ") - " + hades.getMainDataSourceName() + ", " +
                    nanosToMillisHumanReadable(state.getAvgFailover().getValue()) + " (" + state.getMachineState().getLoad().getFailoverDb() + ") - " + hades.getFailoverDataSourceName() +
                    ", using " + (failoverEffectivelyActive ? hades.getFailoverDataSourceName() : hades.getMainDataSourceName()) + (failoverDataSourcePinned != null ? " (PINNED)" : "");
        }
    }

//    public String getLoadLog() {
//        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
//        synchronized (stateGuard) {
//            return nanosToMillisHumanReadable(mainDbAverageNanos.getValue()) + " (" + currentLoad.getMainDb() + ") - " + hades.getMainDataSourceName() + ", " +
//                    nanosToMillisHumanReadable(failoverDbAverageNanos.getValue()) + " (" + currentLoad.getFailoverDb() + ") - " + hades.getFailoverDataSourceName() +
//                    ", using " + (isFailoverEffectivelyActive(failoverDataSourcePinned) ? hades.getFailoverDataSourceName() : hades.getMainDataSourceName()) + (failoverDataSourcePinned != null ? " (PINNED)" : "");
//        }
//    }

//    private boolean isFailoverEffectivelyActive(Boolean failoverDataSourcePinned) {
//        return failoverDataSourcePinned != null ? failoverDataSourcePinned : currentMachineState.isFailoverActive();
//    }

    public MachineState getCurrentState() {
        synchronized (stateGuard) {
            return state.getMachineState();
        }
    }

    public boolean isFailoverActive() {
        synchronized (stateGuard) {
            return state.getMachineState().isFailoverActive();
        }
    }

//    public MachineState getCurrentState() {
//        synchronized (stateGuard) {
//            return currentMachineState;
//        }
//    }
//
//    public boolean isFailoverActive() {
//        synchronized (stateGuard) {
//            return currentMachineState.isFailoverActive();
//        }
//    }

    public void init(Hades haDataSource) {
        this.hades = haDataSource;
        initStmtExecTimeHistories();
        if (cron != null && cron.length() > 0) {
            scheduleByQuartz();
        }
    }

    private void scheduleByQuartz() {
        Scheduler scheduler = getSchedulerCreatingItIfNecessary();
        String quartzSchedulerName = "<UNAVAILABLE>";
        String quartzSchedulerInstanceId = "<UNAVAILABLE>";
        try {
            quartzSchedulerName = scheduler.getSchedulerName();
            quartzSchedulerInstanceId = scheduler.getSchedulerInstanceId();
            hadesQuartzGroup = hadesQuartzGroupPrefix + scheduler.getSchedulerName();
            hadesQuartzJob = hadesQuartzJobPrefix + hades.getMainDataSourceName() + '_' + hades.getFailoverDataSourceName();
            hadesQuartzTrigger = hadesQuartzTriggerPrefix + hades.getMainDataSourceName() + '_' + hades.getFailoverDataSourceName();
            scheduler.deleteJob(hadesQuartzJob, hadesQuartzGroup);
            JobDetail jobDetail = new JobDetail(hadesQuartzJob, hadesQuartzGroup, HadesJob.class);
            jobDetail.setJobDataMap(persistStateInJobDataMap());
            scheduler.scheduleJob(jobDetail, new CronTrigger(hadesQuartzTrigger, hadesQuartzGroup, cron));
            logger.info("trigger with " + hades.getName() + " scheduled on scheduler with name " + quartzSchedulerName + " and instanceId " + quartzSchedulerInstanceId + ": hadesQuartzGroup=" + hadesQuartzGroup + ", hadesQuartzJob=" + hadesQuartzJob + ", hadesQuartzTrigger=" + hadesQuartzTrigger + ", cron=" + cron);
        } catch (ObjectAlreadyExistsException e) {
            logger.error("failed to scheduled trigger with " + hades.getName() + " on scheduler with name " + quartzSchedulerName + " and instanceId " + quartzSchedulerInstanceId + ", hadesQuartzGroup=" + hadesQuartzGroup + ", hadesQuartzJob=" + hadesQuartzJob + ", hadesQuartzTrigger=" + hadesQuartzTrigger + ": " + e.getMessage());
        } catch (SchedulerException e) {
            logger.error("failed to scheduled trigger with " + hades.getName() + " on scheduler with name " + quartzSchedulerName + " and instanceId " + quartzSchedulerInstanceId + ", hadesQuartzGroup=" + hadesQuartzGroup + ", hadesQuartzJob=" + hadesQuartzJob + ", hadesQuartzTrigger=" + hadesQuartzTrigger, e);
        } catch (ParseException e) {
            logger.error("invalid cron expression: " + cron, e);
        }
    }

    private JobDataMap persistStateInJobDataMap() {
        JobDataMap map = new JobDataMap();
        synchronized (stateGuard) {
            map.put(stateKey, state.clone());
        }
        map.put(sqlTimeBasedTriggersIndexKey, sqlTimeBasedTriggersIndex);
        return map;
    }

    private void restoreStateFromJobDataMap(String curRunLogPrefix, JobDataMap map) {
        if (map != null) {
            State oldState;
            State newState;
            synchronized (stateGuard) {
                oldState = state.clone();
                state = ((State) map.get(stateKey)).clone();
                newState = state.clone();
            }
            if (!oldState.equals(newState)) {
                logStateChange(oldState, newState);
            } else {
                logger.debug(curRunLogPrefix + "hades state received from quartz is equal to the current state");
            }
        } else {
            logger.debug("not restoring state from quartz: null job data map");
        }
    }

    private void logStateChange(State oldState, State newState) {
        StringBuilder sb = new StringBuilder("restoring state from quartz job data map; old state:\n");
        appendState(oldState, sb);
        sb.append("\nnew state:\n");
        appendState(newState, sb);
        logger.debug(sb.toString());
    }

    private void appendState(State s, StringBuilder sb) {
        sb.append("currentLoad=")             .append(s.getLoad()).append(',')
          .append("mainDbAverageNanos=")      .append(s.getAvg()).append(',')
          .append("failoverDbAverageNanos=")  .append(s.getAvgFailover()).append(',')
          .append("currentState=")            .append(s.getMachineState()).append(',')
          .append("sqlMainDbTimeHistory=")    .append(s.getHistory()).append(',')
          .append("sqlFailoverDbTimeHistory=").append(s.getHistoryFailover());
    }

    public static class HadesJob implements StatefulJob {
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            JobDataMap map = jobExecutionContext.getMergedJobDataMap();
            int index = map.getInt(sqlTimeBasedTriggersIndexKey);
            SqlTimeBasedTriggerImpl trigger;
            synchronized (sqlTimeBasedTriggers) {
                trigger = sqlTimeBasedTriggers.get(index);
            }
            trigger.run(map);
            jobExecutionContext.getJobDetail().setJobDataMap(trigger.persistStateInJobDataMap());
        }
    }

    public void setSqlTimeTriggeringFailoverMillis(int sqlTimeTriggeringFailoverMillis) {
        this.sqlTimeTriggeringFailoverMillis = sqlTimeTriggeringFailoverMillis;
        this.sqlTimeTriggeringFailoverNanos = Hades.nanosInMillisecond * sqlTimeTriggeringFailoverMillis;
    }

    public void setSqlTimeTriggeringFailbackMillis(int sqlTimeTriggeringFailbackMillis) {
        this.sqlTimeTriggeringFailbackMillis = sqlTimeTriggeringFailbackMillis;
        this.sqlTimeTriggeringFailbackNanos = Hades.nanosInMillisecond * sqlTimeTriggeringFailbackMillis;
    }

    public void setSqlTimesIncludedInAverage(int sqlTimesIncludedInAverage) {
        this.sqlTimesIncludedInAverage = sqlTimesIncludedInAverage;
    }

    public void setExceptionsIgnoredAfterRecovery(boolean exceptionsIgnoredAfterRecovery) {
        this.exceptionsIgnoredAfterRecovery = exceptionsIgnoredAfterRecovery;
    }

    public void setRecoveryErasesHistoryIfExceptionsIgnoredAfterRecovery(boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery) {
        this.recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery = recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery;
    }

    public void setSqlTimeCalculator(SqlTimeCalculator sqlTimeCalculator) {
        this.sqlTimeCalculator = sqlTimeCalculator;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public State getState() {
        synchronized (stateGuard) {
            return state.clone();
        }
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (scheduler != null) {
            try {
                schedulerName = scheduler.getSchedulerName();
                schedulerInstanceId = scheduler.getSchedulerInstanceId();
            } catch (SchedulerException e) {
                String s = "name or instanceId of quartz scheduler could not be retrieved";
                logger.error(s, e);
                throw new RuntimeException(s, e);
            }
            assertNotEmpty(schedulerName, "null or empty scheduler name");
            assertNotEmpty(schedulerName, "null or empty scheduler instanceId");
        }
    }

    private void assertNotEmpty(String s, String errorMessage) {
        if (s == null || s.length() == 0) {
            throw new IllegalStateException(errorMessage + ": " + s);
        }
    }

    private Scheduler getSchedulerCreatingItIfNecessary() {
        if (scheduler == null) {
            try {
                setScheduler(StdSchedulerFactory.getDefaultScheduler());
            } catch (SchedulerException e) {
                String s = "quartz default scheduler could not be retrieved";
                logger.error(s, e);
                throw new RuntimeException(s, e);
            }
        }
        return scheduler;
    }

    public void setIncludeDebuggingInfoInEveryLog(boolean includeDebuggingInfoInEveryLog) {
        this.includeDebuggingInfoInEveryLog = includeDebuggingInfoInEveryLog;
    }
}
