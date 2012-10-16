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

import java.net.UnknownHostException;
import java.util.*;

import static pl.touk.hades.Utils.indent;

/**
* Monitor that is a {@link TimerTask} and uses {@link SqlTimeBasedMonitorImpl} to measure load of the associated hades.
*
* @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
*/
public final class SqlTimeBasedTimerTaskMonitor extends TimerTask implements SqlTimeBasedMonitor {

    static private final Logger logger = LoggerFactory.getLogger(SqlTimeBasedTimerTaskMonitor.class);

    private final SqlTimeBasedMonitorImpl monitor;

    private volatile long endOfLastRunMethod = -1;

    public SqlTimeBasedTimerTaskMonitor(Hades<SqlTimeBasedMonitorImpl> hades,
                                        int sqlTimeTriggeringFailoverMillis,
                                        int sqlTimeTriggeringFailbackMillis,
                                        int sqlTimesIncludedInAverage,
                                        boolean exceptionsIgnoredAfterRecovery,
                                        boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                                        SqlTimeCalculator sqlTimeCalculator,
                                        int currentToUnusedRatio,
                                        int backOffMultiplier,
                                        int backOffMaxRatio,
                                        String host)
            throws UnknownHostException {

        monitor = new SqlTimeBasedMonitorImpl(
                hades,
                sqlTimeTriggeringFailoverMillis,
                sqlTimeTriggeringFailbackMillis,
                sqlTimesIncludedInAverage,
                exceptionsIgnoredAfterRecovery,
                recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery,
                sqlTimeCalculator,
                currentToUnusedRatio,
                backOffMultiplier,
                backOffMaxRatio,
                host);
    }

    public void init() {
        monitor.init(this);
    }

    public void run() {
        String logPrefix = "[" + getHades() + ", scheduledExecTime=" + Utils.tf.format(new Date(scheduledExecutionTime())) + "] ";
        logger.info(logPrefix + "TimerTask started");
        try {
            if (!lastExecutionDelayedThisExecutionOfTimerTask(logPrefix)) {
                try {
                    monitor.run(indent(logPrefix));
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

    public boolean isFailoverActive() {
        return monitor.isFailoverActive();
    }

    public Hades getHades() {
        return monitor.getHades();
    }

    public void connectionRequestedFromHades(boolean success, boolean failover, long timeNanos) {
        // TODO: maybe information about getConnection() result can be used to activate failover/failback early?
    }
}
