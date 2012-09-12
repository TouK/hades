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

import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.StatefulJob;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.Hades;
import pl.touk.hades.Utils;
import pl.touk.hades.load.Load;

import java.lang.Exception;
import java.lang.Integer;
import java.lang.InterruptedException;
import java.lang.Long;
import java.lang.Runnable;
import java.lang.RuntimeException;
import java.lang.String;
import java.lang.System;
import java.lang.Thread;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static pl.touk.hades.Utils.indent;

/**
* Monitor that usesTrigger that activates failover when the main data source
* is overloaded in comparison to the failover data source.
*
* @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
*/
public final class SqlTimeBasedQuartzMonitor implements SqlTimeBasedMonitor {

    static private final Logger logger = LoggerFactory.getLogger(SqlTimeBasedQuartzMonitor.class);

    private final SqlTimeBasedMonitorImpl monitor;

    private final SqlTimeQuartzCalculator calc;
    private final QuartzRepo repo;

    private volatile long startOfLastRunMethod = -1;

    private final String cron;
    private final Scheduler scheduler;
    private final String schedulerName;
    private final String schedulerInstanceId;
    private final static List<SqlTimeBasedQuartzMonitor> sqlTimeBasedMonitors = new ArrayList<SqlTimeBasedQuartzMonitor>();
    private final int sqlTimeBasedMonitorsIndex;
    private final static String sqlTimeBasedMonitorsIndexKey = "sqlTimeBasedMonitorsIndex";
    private final static String stateKey = "sqlTimeBasedTriggerState";
    private final String hadesQuartzGroup;
    private final String hadesQuartzJob;
    private final String hadesQuartzTrigger;
    private final static String hadesQuartzGroupPrefix   = "HADES_GROUP_";
    private final static String hadesQuartzJobPrefix     = "HADES_JOB_";
    private final static String hadesQuartzTriggerPrefix = "HADES_TRIGGER_";
    private final ExecutorService syncExecutor;
    private final long startDelayMillis;

    private final LinkedList<Long> monitoringMethodDurationMillisHistory = new LinkedList<Long>();
    private final int monitoringMethodDurationHistorySize;
    private final long syncAttemptDelayMillis;

    public SqlTimeBasedQuartzMonitor(Hades<SqlTimeBasedMonitorImpl> hades,
                                     int sqlTimeTriggeringFailoverMillis,
                                     int sqlTimeTriggeringFailbackMillis,
                                     int sqlTimesIncludedInAverage,
                                     boolean exceptionsIgnoredAfterRecovery,
                                     boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                                     SqlTimeQuartzCalculator calc,
                                     QuartzRepo repo,
                                     int currentToUnusedRatio,
                                     int backOffMultiplier,
                                     int backOffMaxRatio,
                                     String cron,
                                     long startDelayMillis,
                                     long syncAttemptDelayMillis,
                                     Scheduler scheduler,
                                     ExecutorService syncExecutor,
                                     int monitoringMethodDurationHistorySize)
            throws UnknownHostException, SchedulerException {

        Utils.assertNonEmpty(cron, "cron");
        Utils.assertNonNegative(startDelayMillis, "startDelayMillis");
        Utils.assertNonNegative(syncAttemptDelayMillis, "syncAttemptDelayMillis");
        Utils.assertNotNull(calc, "calc");
        Utils.assertNotNull(repo, "repo");

        this.cron = cron;

        this.schedulerName = scheduler.getSchedulerName();
        this.schedulerInstanceId = scheduler.getSchedulerInstanceId();
        this.hadesQuartzGroup = hadesQuartzGroupPrefix + schedulerName;
        String suffix = hades.getMainDsName() + '_' + hades.getFailoverDsName();
        this.hadesQuartzJob = hadesQuartzJobPrefix + suffix;
        this.hadesQuartzTrigger = hadesQuartzTriggerPrefix + suffix;
        this.scheduler = scheduler;
        this.syncExecutor = syncExecutor;
        this.syncAttemptDelayMillis = syncAttemptDelayMillis;
        this.startDelayMillis = startDelayMillis;
        this.monitoringMethodDurationHistorySize = monitoringMethodDurationHistorySize;
        synchronized (sqlTimeBasedMonitors) {
            this.sqlTimeBasedMonitorsIndex = sqlTimeBasedMonitors.size();
            sqlTimeBasedMonitors.add(null);
        }

        Utils.assertNonEmpty(schedulerName, "scheduler.schedulerName");
        Utils.assertNonEmpty(schedulerInstanceId, "scheduler.schedulerInstanceId");

        this.monitor = new SqlTimeBasedMonitorImpl(
                hades,
                sqlTimeTriggeringFailoverMillis,
                sqlTimeTriggeringFailbackMillis,
                sqlTimesIncludedInAverage,
                exceptionsIgnoredAfterRecovery,
                recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                calc,
                currentToUnusedRatio,
                backOffMultiplier,
                backOffMaxRatio
        );

        this.calc = calc;
        this.repo = repo;
    }

    public void init() {
        monitor.init(this);
        scheduleWithQuartz();
    }

    public void connectionRequestedFromHades(boolean success, boolean failover, long timeNanos) {
        // TODO: maybe information about getConnection() result can be used to activate failover/failback early?
    }

    private void syncPeriodically(Trigger quartzTrigger, Date firstFireTime) {
        Date fireTime = firstFireTime;
        do {
            String curSyncLogPrefix = "[" + monitor.getHades() + ", fireTime=" + Utils.formatTime(fireTime) + ", sync] ";
            logger.info(curSyncLogPrefix + "syncing started");
            try {
                fireTime = syncAndGetNextFireTime(indent(curSyncLogPrefix), quartzTrigger, fireTime);
                logger.info(curSyncLogPrefix + "syncing ended");
            } catch (RuntimeException e) {
                logger.info(curSyncLogPrefix + "syncing ended with exception", e);
                throw e;
            } catch (java.lang.InterruptedException e) {
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
        State copy = monitor.getState();
        if (minPossibleFireTime < copy.getModifyTimeMillis()) {
            logger.info(logPrefix + "attempt " + attempt + ": already synced: " + copy);
            return true;
        }
        long[] measuringDurationMillis = new long[1];
        State state = repo.getHadesClusterState(
                logPrefix + "attempt " + attempt + ": ",
                getHades(),
                minPossibleFireTime,
                measuringDurationMillis
        );
        if (state != null) {
            saveMonitoringMethodDuration(measuringDurationMillis[0]);
            monitor.setState(null, calc.syncValidate(logPrefix, state));
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
            return calc.estimateMaxExecutionTimeMillisOfCalculationMethod();
        }
    }

    private String getSchedulerInfo() {
        return "[" + schedulerName + ", " + schedulerInstanceId + "] ";
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
            storeMonitor();
            scheduler.addJob(jobDetail, false);
            Trigger t = new CronTrigger(hadesQuartzTrigger, hadesQuartzGroup, hadesQuartzJob, hadesQuartzGroup,
                    new Date(System.currentTimeMillis() + startDelayMillis), null, cron);
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
        map.put(stateKey, monitor.getState());
        map.put(sqlTimeBasedMonitorsIndexKey, sqlTimeBasedMonitorsIndex);
        return map;
    }

    private void restoreStateFromJobExecutionContext(String curRunLogPrefix, JobExecutionContext map) {
        State newState = (State) map.getMergedJobDataMap().get(stateKey);
        State oldState = monitor.setState(curRunLogPrefix, newState);
        if (!oldState.equals(newState)) {
            logger.debug(curRunLogPrefix + "restoring state from quartz job data map:\nold: " + oldState + "\nnew: " + newState);
        } else {
            logger.debug(curRunLogPrefix + "hades state received from quartz job data map is equal to the current state");
        }
    }

    public static class HadesJob implements StatefulJob {
        public void execute(JobExecutionContext ctx) throws JobExecutionException {
            SqlTimeBasedQuartzMonitor monitor = extractMonitor(ctx);
            monitor.saveRunMethodStartTime();
            String logPrefix = "[" + monitor.getHades() + ", fireTime=" + Utils.formatTime(ctx.getTrigger().getPreviousFireTime()) + "] ";
            logger.info(logPrefix + "HadesJob started");
            try {
                monitor.restoreStateFromJobExecutionContext(indent(logPrefix), ctx);
                monitor.run(indent(logPrefix), ctx);
                ctx.getJobDetail().setJobDataMap(monitor.createHadesJobDataMap());
                logger.info(logPrefix + "HadesJob ended");
            } catch(RuntimeException e) {
                logger.info(logPrefix + "HadesJob ended with exception", e);
                throw new JobExecutionException(e);
            } catch (InterruptedException e) {
                logger.info(logPrefix + "HadesJob interrupted", e);
                Thread.currentThread().interrupt();
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

    private static SqlTimeBasedQuartzMonitor extractMonitor(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        int index = jobExecutionContext.getMergedJobDataMap().getInt(sqlTimeBasedMonitorsIndexKey);
        SqlTimeBasedQuartzMonitor monitor;
        synchronized (sqlTimeBasedMonitors) {
            if (index < sqlTimeBasedMonitors.size()) {
                monitor = sqlTimeBasedMonitors.get(index);
            } else {
                throw new JobExecutionException("each scheduler of a quartz cluster should have identical hades "
                        + "configuration; for example monitors should be identical, but this is not the case: "
                        + "there is a scheduler with more monitors (at least " + (index + 1) + " monitor(s)) than this "
                        + "scheduler (" + sqlTimeBasedMonitors.size() + " monitor(s))");
            }
        }
        logger.info("monitor at index " + index + ": " + monitor);
        return monitor;
    }

    private void storeMonitor() {
        synchronized (sqlTimeBasedMonitors) {
            sqlTimeBasedMonitors.set(sqlTimeBasedMonitorsIndex, this);
        }
        logger.info("stored monitor at index " + sqlTimeBasedMonitorsIndex + ": " + this);
    }

    public String getCron() {
        return cron;
    }

    void run(String logPrefix, JobExecutionContext ctx) throws InterruptedException {
        State newState = monitor.run(logPrefix);
        repo.saveHadesClusterState(logPrefix, getHades(), newState, ctx.getTrigger().getPreviousFireTime().getTime());
    }

    public boolean isFailoverActive() {
        return monitor.isFailoverActive();
    }

    public Hades getHades() {
        return monitor.getHades();
    }

    public Load getLoad() {
        return monitor.getLoad();
    }

    public String getLoadLog() {
        return monitor.getLoadLog();
    }

    public long getLastQueryTimeMillis(boolean main) {
        return monitor.getLastQueryTimeMillis(main);
    }

    public long getLastQueryTimeNanos(boolean main) {
        return monitor.getLastQueryTimeNanos(main);
    }
}
