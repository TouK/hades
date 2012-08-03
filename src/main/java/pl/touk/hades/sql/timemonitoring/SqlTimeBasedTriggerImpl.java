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

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;
import pl.touk.hades.load.statemachine.MachineState;

import java.util.*;
import java.util.concurrent.ExecutorService;

import static pl.touk.hades.Utils.indent;

/**
* Trigger that activates failover when the main data source
* is overloaded in comparison to the failover data source. The exact algorithm used is described in {@link #run()}.
*
* @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
*/
public final class SqlTimeBasedTriggerImpl extends TimerTask implements SqlTimeBasedTrigger {

    static private final Logger logger = LoggerFactory.getLogger(SqlTimeBasedTriggerImpl.class);

    private volatile long startOfLastRunMethod = -1;
    private volatile long endOfLastRunMethod = -1;

    private final Hades<SqlTimeBasedTriggerImpl> hades;
    private final SqlTimeCalculator sqlTimeCalculator;

    private final String cron;
    private final Scheduler scheduler;
    private final String schedulerName;
    private final String schedulerInstanceId;
    private final static List<SqlTimeBasedTriggerImpl> sqlTimeBasedTriggers = new ArrayList<SqlTimeBasedTriggerImpl>();
    private final int sqlTimeBasedTriggersIndex;
    private final static String sqlTimeBasedTriggersIndexKey = "sqlTimeBasedTriggersIndex";
    private final static String stateKey = "sqlTimeBasedTriggerState";
    private final String hadesQuartzGroup;
    private final String hadesQuartzJob;
    private final String hadesQuartzTrigger;
    private final static String hadesQuartzGroupPrefix   = "HADES_GROUP_";
    private final static String hadesQuartzJobPrefix     = "HADES_JOB_";
    private final static String hadesQuartzTriggerPrefix = "HADES_TRIGGER_";
    private final ExecutorService syncExecutor;
    private final long startDelayMillis;

    private final State state;

    private final LinkedList<Long> monitoringMethodDurationMillisHistory = new LinkedList<Long>();
    private final int monitoringMethodDurationHistorySize;
    private final long syncAttemptDelayMillis;

    public SqlTimeBasedTriggerImpl(Hades<SqlTimeBasedTriggerImpl> hades,
                                   int sqlTimeTriggeringFailoverMillis,
                                   int sqlTimeTriggeringFailbackMillis,
                                   int sqlTimesIncludedInAverage,
                                   boolean exceptionsIgnoredAfterRecovery,
                                   boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                                   SqlTimeCalculator sqlTimeCalculator,
                                   int currentToUnusedRatio,
                                   int backOffMultiplier,
                                   int backOffMaxRatio,
                                   String cron,
                                   long startDelayMillis,
                                   long syncAttemptDelayMillis,
                                   Scheduler scheduler,
                                   ExecutorService syncExecutor,
                                   int monitoringMethodDurationHistorySize) {

        Utils.assertNotNull(hades, "hades");
        Utils.assertNonNegative(sqlTimeTriggeringFailoverMillis, "sqlTimeTriggeringFailoverMillis");
        Utils.assertNonNegative(sqlTimeTriggeringFailbackMillis, "sqlTimeTriggeringFailbackMillis");
        Utils.assertPositive(sqlTimesIncludedInAverage, "sqlTimesIncludedInAverage");
        Utils.assertNotNull(sqlTimeCalculator, "sqlTimeCalculator");
        Utils.assertNull(sqlTimeCalculator.getSqlTimeBasedTrigger(), "sqlTimeCalculator.sqlTimeBasedTrigger");
        Utils.assertNotNull(cron, "cron");
        Utils.assertNonNegative(startDelayMillis, "startDelayMillis");
        Utils.assertNonNegative(syncAttemptDelayMillis, "syncAttemptDelayMillis");

        this.hades = hades;
        this.sqlTimeCalculator = sqlTimeCalculator;
        this.cron = cron;

        this.schedulerName = schedulerName(scheduler);
        this.schedulerInstanceId = schedulerInstanceId(scheduler);
        this.hadesQuartzGroup = hadesQuartzGroupPrefix + schedulerName;
        String suffix = hades.getMainDsName() + '_' + hades.getFailoverDsName();
        this.hadesQuartzJob = hadesQuartzJobPrefix + suffix;
        this.hadesQuartzTrigger = hadesQuartzTriggerPrefix + suffix;
        this.scheduler = scheduler;
        this.syncExecutor = syncExecutor;
        this.syncAttemptDelayMillis = syncAttemptDelayMillis;
        this.startDelayMillis = startDelayMillis;
        this.monitoringMethodDurationHistorySize = monitoringMethodDurationHistorySize;
        synchronized (sqlTimeBasedTriggers) {
            this.sqlTimeBasedTriggersIndex = sqlTimeBasedTriggers.size();
            sqlTimeBasedTriggers.add(null);
        }

        Utils.assertNonEmpty(schedulerName, "scheduler.schedulerName");
        Utils.assertNonEmpty(schedulerInstanceId, "scheduler.schedulerInstanceId");

        state = new State(new SqlTimeBasedLoadFactory(Utils.millisToNanos(sqlTimeTriggeringFailoverMillis),
                                                      Utils.millisToNanos(sqlTimeTriggeringFailbackMillis)),
                          schedulerInstanceId,
                          sqlTimesIncludedInAverage,
                          exceptionsIgnoredAfterRecovery,
                          recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                          currentToUnusedRatio,
                          backOffMultiplier,
                          backOffMaxRatio);
    }

    public SqlTimeBasedTriggerImpl(Hades<SqlTimeBasedTriggerImpl> hades,
                                   int sqlTimeTriggeringFailoverMillis,
                                   int sqlTimeTriggeringFailbackMillis,
                                   int sqlTimesIncludedInAverage,
                                   boolean exceptionsIgnoredAfterRecovery,
                                   boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                                   SqlTimeCalculator sqlTimeCalculator,
                                   int currentToUnusedRatio,
                                   int backOffMultiplier,
                                   int backOffMaxRatio) {

        Utils.assertNotNull(hades, "hades");
        Utils.assertNonNegative(sqlTimeTriggeringFailoverMillis, "sqlTimeTriggeringFailoverMillis");
        Utils.assertNonNegative(sqlTimeTriggeringFailbackMillis, "sqlTimeTriggeringFailbackMillis");
        Utils.assertPositive(sqlTimesIncludedInAverage, "sqlTimesIncludedInAverage");
        Utils.assertNotNull(sqlTimeCalculator, "sqlTimeCalculator");
        Utils.assertNull(sqlTimeCalculator.getSqlTimeBasedTrigger(), "sqlTimeCalculator.sqlTimeBasedTrigger");

        this.hades = hades;
        this.sqlTimeCalculator = sqlTimeCalculator;

        this.cron = null;
        this.schedulerName = null;
        this.schedulerInstanceId = null;
        this.hadesQuartzGroup = null;
        this.hadesQuartzJob = null;
        this.hadesQuartzTrigger = null;
        this.scheduler = null;
        this.syncExecutor = null;
        this.syncAttemptDelayMillis = -1;
        this.startDelayMillis = -1;
        this.monitoringMethodDurationHistorySize = -1;
        this.sqlTimeBasedTriggersIndex = -1;

        state = new State(new SqlTimeBasedLoadFactory(Utils.millisToNanos(sqlTimeTriggeringFailoverMillis),
                                                      Utils.millisToNanos(sqlTimeTriggeringFailbackMillis)),
                          null,
                          sqlTimesIncludedInAverage,
                          exceptionsIgnoredAfterRecovery,
                          recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                          currentToUnusedRatio,
                          backOffMultiplier,
                          backOffMaxRatio);
    }

    public void init() {
        hades.init(this);
        sqlTimeCalculator.init(this);
        scheduleWithQuartz();
    }

    private static String schedulerName(Scheduler scheduler) {
        try {
            return scheduler.getSchedulerName();
        } catch (SchedulerException e) {
            throw new IllegalArgumentException("scheduler name unavailable", e);
        }
    }

    private static String schedulerInstanceId(Scheduler scheduler) {
        try {
            return scheduler.getSchedulerInstanceId();
        } catch (SchedulerException e) {
            throw new IllegalArgumentException("scheduler instance id unavailable", e);
        }
    }

    public void run() {
        String logPrefix = "[" + getHades() + ", scheduledExecTime=" + Utils.tf.format(new Date(scheduledExecutionTime())) + "] ";
        logger.info(logPrefix + "TimerTask started");
        try {
            if (!lastExecutionDelayedThisExecutionOfTimerTask(logPrefix)) {
                try {
                    run(indent(logPrefix), null);
                } finally {
                    ensureThatExecutionsDelayedByThisTimerTaskExecutionAreAbandoned();
                }
            }
            logger.info(logPrefix + "TimerTask ended");
        } catch (RuntimeException e) {
            logger.info(logPrefix + "TimerTask ended with exception", e);
            throw e;
        }
    }

    private void run(String logPrefix, JobExecutionContext quartzCtx) {
        logger.debug(logPrefix + "run started");
        try {
            long[] times = sqlTimeCalculator.calculateMainAndFailoverSqlTimesNanos(indent(logPrefix), getState());
            updateState(indent(logPrefix), times[0], times[1], quartzCtx);
        } catch (InterruptedException e) {
            logger.info(indent(logPrefix) + "measuring interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void connectionRequested(boolean success, boolean failover, long timeNanos) {
        // TODO: maybe information about getConnection() success/failure can be somehow used to activate failover/failback early?
    }

    private void syncPeriodically(Trigger quartzTrigger, Date firstFireTime) {
        Date fireTime = firstFireTime;
        do {
            String curSyncLogPrefix = "[" + hades + ", fireTime=" + Utils.formatTime(fireTime) + ", sync] ";
            logger.info(curSyncLogPrefix + "syncing started");
            try {
                fireTime = syncAndGetNextFireTime(indent(curSyncLogPrefix), quartzTrigger, fireTime);
                logger.info(curSyncLogPrefix + "syncing ended");
            } catch (RuntimeException e) {
                logger.info(curSyncLogPrefix + "syncing ended with exception", e);
                throw e;
            } catch (InterruptedException e) {
                logger.warn(curSyncLogPrefix + "interrupted hence exiting");
                Thread.currentThread().interrupt();
                return;
            }
        } while (true);
    }

    private Date syncAndGetNextFireTime(String logPrefix, Trigger quartzTrigger, Date fireTime) throws InterruptedException {
        sleepUntilQuartzTriggerFiredAndSyncDelayPassed(logPrefix, fireTime);
        int attempt = 1;
        do {
            if (syncStateWithHadesCluster(logPrefix, fireTime, attempt)) {
                break;
            } else {
                if (syncAttemptDelayMillis > 0) {
                    Thread.sleep(syncAttemptDelayMillis);
                }
                attempt++;
                if (attempt % 10 == 0) {
                    checkScheduler(logPrefix);
                }
            }
        } while (true);
        return computeNextFireTimeAvoidingMisfires(fireTime, quartzTrigger);

    }

    private void checkScheduler(String logPrefix) {
        logger.info(logPrefix + " checking why cannot sync...");
        logPrefix = indent(logPrefix);

        try {
            logger.info(logPrefix + scheduler.getMetaData().getSummary());
            if (scheduler.isInStandbyMode()) {
                logger.error("scheduler is in stand-by mode - starting it");
                scheduler.start();
                if (!scheduler.isInStandbyMode()) {
                    logger.info("scheduler started and no longer in stand-by mode");
                } else {
                    logger.info("scheduler still in stand-by mode");
                }
            }
            JobDetail d = findHadesQuartzJob();
            if (d != null) {
                logger.info(logPrefix + "found hades quartz job: " + d.toString());
            } else {
                logger.error(logPrefix + "no hades quartz job found");
            }
            String[] triggerState = new String[1];
            Trigger t = findHadesQuartzTrigger(triggerState);
            if (t != null) {
                logger.error(logPrefix + "found hades quartz trigger" + t.toString() + " with state " + triggerState[0]);
                logger.info(logPrefix + "previous fire time: " + t.getPreviousFireTime());
            } else {
                logger.error(logPrefix + "no hades quartz trigger");
            }
            JobExecutionContext executingHades = hadesJobIsExecuting();
            if (executingHades != null) {
                logger.info(logPrefix + "hades job is executing since " + Utils.format(executingHades.getFireTime()));
            } else {
                logger.info(logPrefix + "hades job is currently not executing");
            }
        } catch (SchedulerException e) {
            logger.error(logPrefix + "unexpected exception", e);
        }
    }

    private JobExecutionContext hadesJobIsExecuting() throws SchedulerException {
        for (JobExecutionContext c: (List<JobExecutionContext>) scheduler.getCurrentlyExecutingJobs()) {
            if (hadesQuartzJob.equals(c.getJobDetail().getName()) && hadesQuartzGroup.equals(c.getJobDetail().getGroup())) {
                return c;
            }
        }
        return null;
    }

    private JobDetail findHadesQuartzJob() throws SchedulerException {
        return scheduler.getJobDetail(hadesQuartzJob, hadesQuartzGroup);
    }

    private Trigger findHadesQuartzTrigger(String[] triggerState) throws SchedulerException {
        int i = scheduler.getTriggerState(hadesQuartzTrigger, hadesQuartzGroup);
        switch (i) {
            case Trigger.STATE_BLOCKED : triggerState[0] = "STATE_BLOCKED"; break;
            case Trigger.STATE_COMPLETE: triggerState[0] = "STATE_COMPLETE";break;
            case Trigger.STATE_ERROR   : triggerState[0] = "STATE_ERROR";   break;
            case Trigger.STATE_NONE    : triggerState[0] = "STATE_NONE";    break;
            case Trigger.STATE_NORMAL  : triggerState[0] = "STATE_NORMAL";  break;
            case Trigger.STATE_PAUSED  : triggerState[0] = "STATE_PAUSED";  break;
            default: triggerState[0] = Integer.toString(i); break;
        }
        return scheduler.getTrigger(hadesQuartzTrigger, hadesQuartzGroup);
    }

    private Date computeNextFireTimeAvoidingMisfires(Date fireTime, Trigger quartzTrigger) {
        return quartzTrigger.getFireTimeAfter(getDateOneSecondAgoButAfter(fireTime));
    }

    private Date getDateOneSecondAgoButAfter(Date date) {
        long secondAgo = new Date().getTime() - 1000;
        if (secondAgo > date.getTime()) {
            return new Date(secondAgo);
        } else {
            return new Date(date.getTime() + 1);
        }
    }

    private void sleepUntilQuartzTriggerFiredAndSyncDelayPassed(String curSyncLogPrefix, Date nearestFireTime) throws InterruptedException {
        long delay = calculateSyncDelayMillis();
        long nextSyncTime = nearestFireTime.getTime() + delay;
        long now = System.currentTimeMillis();
        if (nextSyncTime > now) {
            logger.info(curSyncLogPrefix + "sleeping until fireTime + delay = " + Utils.df.format(new Date(nextSyncTime)) + " (calculated delay: " + delay + " ms)");
            Thread.sleep(nextSyncTime - now);
            logger.info(curSyncLogPrefix + "continuing syncing after sleep");
        } else {
            // Strange situation. Nothing we can do about it. Just return.
            logger.warn(curSyncLogPrefix + "calculated nextSyncTime=" + Utils.format(new Date(nextSyncTime)) + " is not after now=" + Utils.format(new Date(now)));
        }
    }

    private boolean syncStateWithHadesCluster(String logPrefix, Date fireTime, int attempt) throws InterruptedException {
        long minPossibleFireTime = fireTime.getTime() - 1000;
        if (minPossibleFireTime < startOfLastRunMethod) {
            logger.info(logPrefix + "attempt " + attempt + ": sync not needed: run method is executing locally");
            return true;
        }
        boolean synced = false;
        State copy = null;
        synchronized (state) {
            if (minPossibleFireTime < state.getModifyTimeMillis()) {
                synced = true;
                copy = state.clone();
            }
        }
        if (synced) {
            logger.info(logPrefix + "attempt " + attempt + ": already synced: " + copy);
            return true;
        }
        long[] measuringDurationMillis = new long[1];
        State state = sqlTimeCalculator.getSqlTimeRepo().getHadesClusterState(logPrefix + "attempt " + attempt + ": ", minPossibleFireTime, measuringDurationMillis);
        if (state != null) {
            saveMonitoringMethodDuration(measuringDurationMillis[0]);
            setState(null, sqlTimeCalculator.syncValidate(logPrefix, state));
            return true;
        } else {
            return false;
        }
    }

    private long calculateSyncDelayMillis() {
        long statisticalMaxMillis = getMaxMonitoringDurationMillis();
        if (statisticalMaxMillis > 0) {
            return statisticalMaxMillis;
        } else {
            return sqlTimeCalculator.estimateMaxExecutionTimeMillisOfCalculationMethod();
        }
    }

    private String getSchedulerInfo() {
        return "[" + schedulerName + ", " + schedulerInstanceId + "] ";
    }

    private void updateState(String logPrefix, long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos, JobExecutionContext quartzCtx) throws InterruptedException {
        State oldState;
        State newState;
        synchronized (state) {
            oldState = state.clone();
            state.updateLocalStateWithNewExecTimes(indent(logPrefix), getHades(), mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos, schedulerInstanceId);
            newState = state.clone();
        }
        if (newState.getMachineState().isFailoverActive() != oldState.getMachineState().isFailoverActive()) {
            warnAboutFailbackOrFailover(logPrefix, oldState, newState);
        } else {
            infoAboutPreservingFailoverOrFailback(logPrefix, oldState, newState);
        }
        if (quartzCtx != null) {
            sqlTimeCalculator.getSqlTimeRepo().saveHadesClusterState(logPrefix, newState, quartzCtx.getTrigger().getPreviousFireTime().getTime());
        }
    }

    public long getLastFailoverQueryTimeMillis(boolean main) {
        long nanos;
        synchronized (state) {
            nanos = (main ? state.getAvg() : state.getAvgFailover()).getLast();
        }
        return Utils.nanosToMillis(nanos);
    }

    private String createTransitionDesc(State oldState, State newState) {
        boolean failoverOrFailback = newState.getMachineState().isFailoverActive() != oldState.getMachineState().isFailoverActive();
        String dbSwitchType;
        if (failoverOrFailback) {
            dbSwitchType = newState.getMachineState().isFailoverActive() ? "activating failover (" + hades.getMainDsName() + " -> " + hades.getFailoverDsName() + ")" : "activating failback (" + hades.getFailoverDsName() + " -> " + hades.getMainDsName() + ")";
        } else {
            if (newState.getMachineState().isFailoverActive()) {
                dbSwitchType = "failover remains active (keep using " + hades.getFailoverDsName() + ")";
            } else {
                dbSwitchType = "failover remains inactive (keep using " + hades.getMainDsName() + ")";
            }
        }
        return dbSwitchType;
    }

    private boolean lastExecutionDelayedThisExecutionOfTimerTask(String curRunLogPrefix) {
        long scheduledExecutionTime = scheduledExecutionTime();
        if (scheduledExecutionTime > 0) {
            long endOfLastRunMethodCopy = endOfLastRunMethod;
            if (scheduledExecutionTime < endOfLastRunMethodCopy) {
                logger.warn(curRunLogPrefix + "last execution ended at " + Utils.format(new Date(endOfLastRunMethodCopy)) +
                        " which is after this execution's scheduled time: " + Utils.format(new Date(scheduledExecutionTime)) +
                        "; therefore this execution is abandoned");
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private void ensureThatExecutionsDelayedByThisTimerTaskExecutionAreAbandoned() {
        endOfLastRunMethod = System.currentTimeMillis();
    }

    private void warnAboutFailbackOrFailover(String curRunLogPrefix, State oldState, State newState) {
        logger.warn(curRunLogPrefix + getLog(oldState, newState));
    }

    private void infoAboutPreservingFailoverOrFailback(String curRunLogPrefix, State oldState, State newState) {
        logger.info(curRunLogPrefix + getLog(oldState, newState));
    }

    private String getLog(State oldState, State newState) {
        Boolean failoverDataSourcePinned = hades.getFailoverDataSourcePinned();
        return "average execution time for '" + sqlTimeCalculator.getSql() + "' for last " + newState.getAvg().getItemsCountIncludedInAverage() + " execution(s): "
                + Utils.nanosToMillisAsStr(newState.getAvg().getValue()) + " (last: " + Utils.nanosToMillisAsStr(newState.getAvg().getLast()) + ") - " + hades.getMainDsName() + ", "
                + Utils.nanosToMillisAsStr(newState.getAvgFailover().getValue()) + " (last: " + Utils.nanosToMillisAsStr(newState.getAvgFailover().getLast()) + ") - " + hades.getFailoverDsName()
                + "; load levels derived from average execution times: "
                + getLoadLevels(newState) + "; "
                + createTransitionDesc(oldState, newState)
                + (failoverDataSourcePinned != null ? "; above calculations currently have no effect because " + (failoverDataSourcePinned ? "failover data source (" + hades.getFailoverDsName() + ")" : "main data source (" + hades.getMainDsName() + ")") + " is pinned and therefore used" : "");
    }

    private String getLoadLevels(State state) {
        Load load = state.getMachineState().getLoad();
        return load.getMainDb()     + " - " + hades.getMainDsName() + ", " +
               load.getFailoverDb() + " - " + hades.getFailoverDsName() +
               (load.isMainDbLoadHigher() != null ? " (" + hades.getMainDsName() + " load level is" + (load.isMainDbLoadHigher() ? "" : " not") + " higher)" : "");
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

    public MachineState getCurrentState() {
        synchronized (state) {
            return state.getMachineState();
        }
    }

    public boolean isFailoverActive() {
        synchronized (state) {
            return state.getMachineState().isFailoverActive();
        }
    }

    public void scheduleWithQuartz() {
        if (cron != null) {
            final Date[] firstFireTime = new Date[1];
            final Trigger trigger = schedule(firstFireTime);
            syncExecutor.execute(new Runnable() {
                public void run() {
                    try {
                        syncPeriodically(trigger, firstFireTime[0]);
                    } catch (RuntimeException e) {
                        logger.error("syncPeriodically threw unexpected exception", e);
                    }
                }
            });
        }
    }

    private Trigger schedule(Date[] firstFireTime) {
        String s = hadesQuartzJob + " with " + hadesQuartzTrigger + " on " + getSchedulerInfo() + "with cron '" + cron + "'";
        try {
            scheduler.deleteJob(hadesQuartzJob, hadesQuartzGroup);
            JobDetail jobDetail = new JobDetail(hadesQuartzJob, hadesQuartzGroup, HadesJob.class, false, true, false);
            jobDetail.setJobDataMap(createHadesJobDataMap());
            storeTrigger();
            scheduler.addJob(jobDetail, false);
            Trigger t = new CronTrigger(hadesQuartzTrigger, hadesQuartzGroup, hadesQuartzJob, hadesQuartzGroup, new Date(System.currentTimeMillis() + startDelayMillis), null, cron);
            firstFireTime[0] = scheduler.scheduleJob(t);
            logger.info("scheduled with first fire time " + Utils.format(firstFireTime[0]) + ": " + s);
            return t;
        } catch (Exception e) {
            logger.error("failed to schedule " + s, e);
            throw new RuntimeException(e);
        }
    }

    private JobDataMap createHadesJobDataMap() {
        JobDataMap map = new JobDataMap();
        map.put(stateKey, getState());
        map.put(sqlTimeBasedTriggersIndexKey, sqlTimeBasedTriggersIndex);
        return map;
    }

    private void restoreStateFromJobExecutionContext(String curRunLogPrefix, JobExecutionContext map) {
        State newState = (State) map.getMergedJobDataMap().get(stateKey);
        State oldState = setState(curRunLogPrefix, newState);
        if (!oldState.equals(newState)) {
            logger.debug(curRunLogPrefix + "restoring state from quartz job data map:\nold: " + oldState + "\nnew: " + newState);
        } else {
            logger.debug(curRunLogPrefix + "hades state received from quartz job data map is equal to the current state");
        }
    }

    private State setState(String logPrefixIfFullState, State newState) {
        State oldState;
        synchronized (state) {
            oldState = state.clone();
            state.copyFrom(logPrefixIfFullState, newState);
        }
        return oldState;
    }

    public static class HadesJob implements StatefulJob {
        public void execute(JobExecutionContext ctx) throws JobExecutionException {
            SqlTimeBasedTriggerImpl trigger = extractTrigger(ctx);
            trigger.saveRunMethodStartTime();
            String logPrefix = "[" + trigger.getHades() + ", fireTime=" + Utils.formatTime(ctx.getTrigger().getPreviousFireTime()) + "] ";
            logger.info(logPrefix + "HadesJob started");
            try {
                trigger.restoreStateFromJobExecutionContext(indent(logPrefix), ctx);
                trigger.run(indent(logPrefix), ctx);
                ctx.getJobDetail().setJobDataMap(trigger.createHadesJobDataMap());
                logger.info(logPrefix + "HadesJob ended");
            } catch(RuntimeException e) {
                logger.info(logPrefix + "HadesJob ended with exception", e);
                throw new JobExecutionException(e);
            }
        }
    }

    private void saveRunMethodStartTime() {
        startOfLastRunMethod = System.currentTimeMillis();
    }

    private void saveMonitoringMethodDuration(long durationMillis) {
        Utils.assertNonNegative(durationMillis, "durationMillis");

        synchronized (monitoringMethodDurationMillisHistory) {
            monitoringMethodDurationMillisHistory.add(durationMillis);
            if (monitoringMethodDurationMillisHistory.size() > monitoringMethodDurationHistorySize) {
                monitoringMethodDurationMillisHistory.remove();
            }
        }
    }

    private long getMaxMonitoringDurationMillis() {
        long max = 0;
        synchronized (monitoringMethodDurationMillisHistory) {
            for (long l : monitoringMethodDurationMillisHistory) {
                if (l > max) {
                    max = l;
                }
            }
        }
        return max;
    }

    private static SqlTimeBasedTriggerImpl extractTrigger(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        int index = jobExecutionContext.getMergedJobDataMap().getInt(sqlTimeBasedTriggersIndexKey);
        SqlTimeBasedTriggerImpl t;
        synchronized (sqlTimeBasedTriggers) {
            if (index < sqlTimeBasedTriggers.size()) {
                t = sqlTimeBasedTriggers.get(index);
            } else {
                throw new JobExecutionException("each scheduler of a quartz cluster should have identical hades "
                        + "configuration; for example hades triggers should be identical, but this is not the case: "
                        + "there is a scheduler with more triggers (at least " + (index + 1) + " trigger(s)) than this "
                        + "scheduler (" + sqlTimeBasedTriggers.size() + " trigger(s))");
            }
        }
        logger.info("trigger at index " + index + ": " + t);
        return t;
    }

    private void storeTrigger() {
        synchronized (sqlTimeBasedTriggers) {
            sqlTimeBasedTriggers.set(sqlTimeBasedTriggersIndex, this);
        }
        logger.info("stored trigger at index " + sqlTimeBasedTriggersIndex + ": " + this);
    }

    public State getState() {
        synchronized (state) {
            return state.clone();
        }
    }

    public SqlTimeCalculator getSqlTimeCalculator() {
        return sqlTimeCalculator;
    }

    public Hades getHades() {
        return hades;
    }

    public String getCron() {
        return cron;
    }

    public String getSchedulerInstanceId() {
        return schedulerInstanceId;
    }

    public String getQuartzCluster() {
        return schedulerName;
    }
}
