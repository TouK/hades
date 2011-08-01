/*
 * Copyright 2011 TouK
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
package pl.touk.hades.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static pl.touk.hades.load.LoadLevel.low;
import static pl.touk.hades.load.LoadLevel.medium;
import static pl.touk.hades.load.LoadLevel.high;
import static pl.touk.hades.load.LoadLevel.exceptionWhileMeasuring;
import static pl.touk.hades.load.LoadLevel.notMeasuredYet;
import pl.touk.hades.FailoverActivator;
import pl.touk.hades.HaDataSource;

/**
 * A failover activator that activates failover when the main data source
 * is overloaded in comparison to the failover data source. The exact algorithm used is described in {@link #run()}.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class LoadFailoverActivator extends TimerTask implements FailoverActivator {

    static private final Logger logger = LoggerFactory.getLogger(LoadFailoverActivator.class);

    private String statement = "select sysdate from dual";
    private long stmtExecTimeLimitTriggeringFailoverMillis = 100;
    private long stmtExecTimeLimitTriggeringFailoverNanos = stmtExecTimeLimitTriggeringFailoverMillis * HaDataSource.nanosInMillisecond;
    private int statementExecutionTimeLimitTriggeringFailbackMillis = 50;
    private long statementExecutionTimeLimitTriggeringFailbackNanos = statementExecutionTimeLimitTriggeringFailbackMillis * HaDataSource.nanosInMillisecond;

    private int statementExecutionTimesIncludedInAverage = 1;
    private int statementExecutionTimeout = -1;
    private int connectionGettingTimeout = -1;
    private SqlExecutionTimeHistory stmtMainDbExecTimeHistory = null;
    private SqlExecutionTimeHistory stmtFailoverDbExecTimeHistory = null;

    private boolean exceptionsIgnoredAfterRecovery = true;
    private boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery = true;

    private volatile long endOfLastRunMethod = 0;

    private final DecimalFormat decimalFormat = new DecimalFormat("#0.000 ms");

    private LoadFactory loadFactory;

    // Guards everything related to the current state, i.e.:
    // - stateMachine holding the current state,
    // - currentLoad used to calculate the current state by the stateMachine,
    // - mainDbAverageNanos and failoverDbAverageNanos used to calculate the current load.
    private final Object stateGuard = new Object();
    private Load currentLoad;
    private Average mainDbAverageNanos;
    private Average failoverDbAverageNanos;
    private final StateMachine stateMachine;
    {
        ArrayList<Transition> transitions = new ArrayList<Transition>();

        // All states when failover is inactive:
        HashSet<State> mainDbStates = new HashSet<State>();
        mainDbStates.add(new State(false, low));
        mainDbStates.add(new State(false, low, medium));
        mainDbStates.add(new State(false, low, high));
        mainDbStates.add(new State(false, low, exceptionWhileMeasuring));
        mainDbStates.add(new State(false, medium, low));
        mainDbStates.add(new State(false, medium));
        mainDbStates.add(new State(false, medium, high));
        mainDbStates.add(new State(false, medium, exceptionWhileMeasuring));
        mainDbStates.add(new State(false, high, false));
        mainDbStates.add(new State(false, high, exceptionWhileMeasuring));
        mainDbStates.add(new State(false, exceptionWhileMeasuring, exceptionWhileMeasuring));
        mainDbStates.add(new State(false, notMeasuredYet));
        // All possible transitions between above states keep failover inactive:
        StateMachine.appendAllPossibleTransitionsBetweenStates(mainDbStates, transitions);

        // All states when failover is active:
        HashSet<State> failoverDbStates = new HashSet<State>();
        failoverDbStates.add(new State(true, exceptionWhileMeasuring, low));
        failoverDbStates.add(new State(true, exceptionWhileMeasuring, medium));
        failoverDbStates.add(new State(true, exceptionWhileMeasuring, high));
        failoverDbStates.add(new State(true, high, low));
        failoverDbStates.add(new State(true, high, medium));
        failoverDbStates.add(new State(true, high, true));
        failoverDbStates.add(new State(true, medium, low));
        failoverDbStates.add(new State(true, medium));
        // All possible transitions between above states keep failover active:
        StateMachine.appendAllPossibleTransitionsBetweenStates(failoverDbStates, transitions);

        // Activate failover when main database load becomes high and the failover database works and its load is smaller:
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, high, low), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, high, medium), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, high, true), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, exceptionWhileMeasuring, low), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, exceptionWhileMeasuring, medium), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(mainDbStates, new State(true, exceptionWhileMeasuring, high), transitions);

        // Activate failback when main database load returns to low or becomes smaller than the high failover database load:
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low, medium), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low, high), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, medium, high), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, high, false), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, low, exceptionWhileMeasuring), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, medium, exceptionWhileMeasuring), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, high, exceptionWhileMeasuring), transitions);
        StateMachine.appendAllPossibleTransitionsFromStates(failoverDbStates, new State(false, exceptionWhileMeasuring, exceptionWhileMeasuring), transitions);

        stateMachine = new StateMachine(transitions, new State(false, notMeasuredYet));
    }

    private HaDataSource haDataSource;

    private DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss,SSS");

    /**
     * Calculates current load for the controlled {@link HaDataSource} and activates failover or failback if needed.
     * <p>
     * This method should be invoked periodically. In every invocation the current load is got from a
     * load factory created with constructor
     * {@link LoadFactory#LoadFactory(long, long) LoadFactory(stmtExecTimeLimitTriggeringFailoverMillis, statementExecutionTimeLimitTriggeringFailbackMillis)},
     * where values used as parameters are those set in {@link #setStatementExecutionTimeLimitTriggeringFailoverMillis(int)}
     * and {@link #setStatementExecutionTimeLimitTriggeringFailbackMillis(int)}. The current load is produced through
     * invocation of
     * {@link LoadFactory#getLoad(long, long) getLoad(averageMainDsExecTime, averageFailoverDsExecTime)} on the above
     * factory,
     * where averages used as parameters are calculated for both data sources for last <i>N</i>
     * (which can be set in {@link #setStatementExecutionTimesIncludedInAverage(int)}) execution times of an sql
     * statement (which can be set in {@link #setStatement(String)}. During each invocation of this method the sql
     * statement is executed on the main data source and the failover data source and the execution times are measured.
     * These two execution times are included in above averages passed to the factory.
     * <p>
     * When current load is known a decision is made whether failover or failback should be activated as follows.
     * Failover is activated when main database load becomes high and the failover database works and its load is
     * smaller. Failback is activated when main database load returns to low or becomes smaller than the high failover
     * database load.
     */
    public void run() {
        String curRunLogPrefix = "[" + haDataSource.toString() + ", ts=" + System.currentTimeMillis() + "] ";

        if (logger.isDebugEnabled()) {
            logger.debug(curRunLogPrefix + "start of method measuring load levels");
        }

        if (!lastExecutionDelayedThisExecution(curRunLogPrefix)) {
            try {
                long mainDbStmtExecTimeNanos = measureStatementExecutionTimeNanos(curRunLogPrefix, false);
                long failoverDbStmtExecTimeNanos = measureStatementExecutionTimeNanos(curRunLogPrefix, true);
                updateState(curRunLogPrefix, mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos);

                ensureThatExecutionsDelayedByThisExecutionAreAbandoned();
            } catch (InterruptedException e) {
                logger.error(curRunLogPrefix + "measuring interrupted");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void updateState(String curRunLogPrefix, long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos) {
        synchronized (stateGuard) {
            updateAveragesAndCurrentLoad(mainDbStmtExecTimeNanos, failoverDbStmtExecTimeNanos);
            makeStateTransitionCorrespondingToCurrentLoad(curRunLogPrefix);
        }
    }

    private void makeStateTransitionCorrespondingToCurrentLoad(String curRunLogPrefix) {
        State oldState = stateMachine.getCurrentState();
        if (stateMachine.transition(currentLoad)) {
            informAboutFailbackOrWarnAboutFailover(curRunLogPrefix, oldState, stateMachine.getCurrentState().isFailoverActive());
        } else {
            debugNeitherFailoverNorFailback(curRunLogPrefix, oldState);
        }
    }

    private void updateAveragesAndCurrentLoad(long mainDbStmtExecTimeNanos, long failoverDbStmtExecTimeNanos) {
        mainDbAverageNanos = stmtMainDbExecTimeHistory.updateAverage(mainDbStmtExecTimeNanos);
        failoverDbAverageNanos = stmtFailoverDbExecTimeHistory.updateAverage(failoverDbStmtExecTimeNanos);
        currentLoad = loadFactory.getLoad(mainDbAverageNanos.getValue(), failoverDbAverageNanos.getValue());
    }

    private void initStmtExecTimeHistories() {
        stmtMainDbExecTimeHistory = new SqlExecutionTimeHistory(statementExecutionTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        stmtFailoverDbExecTimeHistory = new SqlExecutionTimeHistory(statementExecutionTimesIncludedInAverage, exceptionsIgnoredAfterRecovery, recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);
        loadFactory = new LoadFactory(stmtExecTimeLimitTriggeringFailoverNanos, statementExecutionTimeLimitTriggeringFailbackNanos);
    }

    private String createTransitionDesc(State oldState) {
        State currentState = stateMachine.getCurrentState();
        boolean failoverOrFailback = currentState.isFailoverActive() != oldState.isFailoverActive();
        String dbSwitchType;
        if (failoverOrFailback) {
            dbSwitchType = currentState.isFailoverActive() ? "activating failover (" + haDataSource.getMainDataSourceName() + " -> " + haDataSource.getFailoverDataSourceName() + ")" : "activating failback (" + haDataSource.getFailoverDataSourceName() + " -> " + haDataSource.getMainDataSourceName() + ")";
        } else {
            if (currentState.isFailoverActive()) {
                dbSwitchType = "failover remains active (keep using " + haDataSource.getFailoverDataSourceName() + ")";
            } else {
                dbSwitchType = "failover remains inactive (keep using " + haDataSource.getMainDataSourceName() + ")";
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

    private void informAboutFailbackOrWarnAboutFailover(String curRunLogPrefix, State oldState, boolean failover) {
        if (failover) {
            logger.warn(curRunLogPrefix + getLog(oldState));
        } else {
            logger.info(curRunLogPrefix + getLog(oldState));
        }
    }

    private void debugNeitherFailoverNorFailback(String curRunLogPrefix, State oldState) {
        logger.debug(curRunLogPrefix + getLog(oldState));
    }

    private String getLog(State oldState) {
        Boolean failoverDataSourcePinned = haDataSource.getFailoverDataSourcePinned();
        return "average execution time for '" + statement + "' for last " + mainDbAverageNanos.getItemsCountIncludedInAverage() + " execution(s): "
                + nanosToMillis(mainDbAverageNanos.getValue())     + " (last: " + nanosToMillis(mainDbAverageNanos.getLast())     + ") - " + haDataSource.getMainDataSourceName() + ", "
                + nanosToMillis(failoverDbAverageNanos.getValue()) + " (last: " + nanosToMillis(failoverDbAverageNanos.getLast()) + ") - " + haDataSource.getFailoverDataSourceName()
                + "; load levels derived from average execution times: "
                + getLoadLevels() + "; "
                + createTransitionDesc(oldState)
                + (failoverDataSourcePinned != null ? "; above calculations currently have no effect because " + (failoverDataSourcePinned ? "failover data source (" + haDataSource.getFailoverDataSourceName() + ")" : "main data source (" + haDataSource.getMainDataSourceName() + ")") + " is pinned and therefore used" : "");
    }

    private String getLoadLevels() {
        Load load = stateMachine.getCurrentState().getLoad();
        return stateMachine.getCurrentState().getLoad().getMainDb()     + " - " + haDataSource.getMainDataSourceName() + ", "
             + stateMachine.getCurrentState().getLoad().getFailoverDb() + " - " + haDataSource.getFailoverDataSourceName()
             + (load.isMainDbLoadHigher() != null ?
                  " (" + haDataSource.getMainDataSourceName() + " load level is" + (load.isMainDbLoadHigher() ? "" : " not") + " higher)"
                : "");
    }

    private String nanosToMillis(long l) {
        if (l < Long.MAX_VALUE) {
            return decimalFormat.format(((double) l) / HaDataSource.nanosInMillisecond);
        } else {
            return "<N/A because of exception(s) - assuming infinity>";
        }
    }

    private long measureStatementExecutionTimeNanos(String curRunLogPrefix, boolean failover) throws InterruptedException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Long queryStart = null;
        long start = System.nanoTime();
        try {
            connection = getConnection(failover, curRunLogPrefix);
            preparedStatement = prepareStatement(connection, failover, curRunLogPrefix);
            if (preparedStatement == null) {
                return Long.MAX_VALUE;
            }
            queryStart = System.nanoTime();
            preparedStatement.execute();
            return System.nanoTime() - queryStart;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            return logException(e, curRunLogPrefix, failover, start,  queryStart, System.nanoTime());
        } finally {
            close(curRunLogPrefix, preparedStatement, connection, failover);
        }
    }
    private long logException(Exception e, String curRunLogPrefix, boolean failover, long start, Long queryStart, long exceptionMoment) {
        if (queryStart == null) {
            logger.error(curRunLogPrefix + "exception while preparing to measure statement execution time on " + getDesc(failover) + " caught in " + nanosToMillis(exceptionMoment - start) + " ms since the beginning of the preparation", e);
        } else {
            logger.error(curRunLogPrefix + "exception while measuring statement execution time on " + getDesc(failover) + " caught in " + nanosToMillis(exceptionMoment - queryStart) + " ms", e);
        }
        return Long.MAX_VALUE;
    }

    private PreparedStatement prepareStatement(Connection connection, boolean failover, String curRunLogPrefix) {
        if (connection != null) {
            try {
                PreparedStatement s = connection.prepareStatement(statement);
                if (statementExecutionTimeout >= 0) {
                    s.setQueryTimeout(statementExecutionTimeout);
                }
                return s;
            } catch (SQLException e) {
                logger.error(curRunLogPrefix + "exception while preparing statement or setting query timeout for measuring execution time on " + getDesc(failover), e);
            }
        }
        return null;
    }

    private Connection getConnection(final boolean failover, final String curRunLogPrefix) throws InterruptedException {
        if (connectionGettingTimeout > 0) {
            return getConnectionWithTimeout(failover,  curRunLogPrefix);
        } else {
            try {
                return haDataSource.getConnection(failover, false, null, null, curRunLogPrefix);
            } catch (SQLException e) {
                return null;
            }
        }
    }

    private Connection getConnectionWithTimeout(final boolean failover, final String curRunLogPrefix) throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Connection> future = executor.submit(new Callable<Connection>() {
            public Connection call() {
                try {
                    return haDataSource.getConnection(failover, false, null, null, curRunLogPrefix);
                } catch (SQLException e) {
                    return null;
                }
            }
        });
        return getConnection(future, executor, failover, curRunLogPrefix);
    }

    private Connection getConnection(Future<Connection> future, ExecutorService executor, boolean failover, String curRunLogPrefix) throws InterruptedException {
        try {
            try {
                return future.get(connectionGettingTimeout, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                logger.error(curRunLogPrefix + "exception while getting connection to " + getDesc(failover), e);
                return null;
            } catch (TimeoutException e) {
                logger.error(curRunLogPrefix + "could not get connection to " + getDesc(failover) + " in " + connectionGettingTimeout + " second(s); assuming that the data source is unavailable", e);
                return null;
            }
        } finally {
            future.cancel(false);
            executor.shutdown();
        }
    }

    private String getDesc(boolean failover) {
        return (failover ? haDataSource.getFailoverDataSourceName() + " (failover" : haDataSource.getMainDataSourceName() + " (main") + " ds)";
    }

    private void close(String curRunLogPrefix, PreparedStatement preparedStatement, Connection connection, boolean failover) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                logger.error(curRunLogPrefix + "exception while closing prepared statement for " + getDesc(failover) + " after measuring the execution time", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error(curRunLogPrefix + "exception while closing connection to " + getDesc(failover) + " after measuring the execution time", e);
            }
        }
    }

    public String getLoadLog() {
        Boolean failoverDataSourcePinned = haDataSource.getFailoverDataSourcePinned();
        synchronized (stateGuard) {
            return nanosToMillis(mainDbAverageNanos.getValue())     + " (" + currentLoad.getMainDb()     + ") - " + haDataSource.getMainDataSourceName() + ", " +
                   nanosToMillis(failoverDbAverageNanos.getValue()) + " (" + currentLoad.getFailoverDb() + ") - " + haDataSource.getFailoverDataSourceName() +
                   ", using " + (isFailoverEffectivelyActive(failoverDataSourcePinned) ? haDataSource.getFailoverDataSourceName() : haDataSource.getMainDataSourceName()) + (failoverDataSourcePinned != null ? " (PINNED)" : "");
        }
    }

    private boolean isFailoverEffectivelyActive(Boolean failoverDataSourcePinned) {
        return failoverDataSourcePinned != null ? failoverDataSourcePinned : stateMachine.getCurrentState().isFailoverActive();
    }

    public State getCurrentState() {
        synchronized (stateGuard) {
            return stateMachine.getCurrentState();
        }
    }

    public boolean isFailoverActive() {
        synchronized (stateGuard) {
            return stateMachine.getCurrentState().isFailoverActive();
        }
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public void init(HaDataSource haDataSource) {
        this.haDataSource = haDataSource;
        initStmtExecTimeHistories();
    }

    public void setStatementExecutionTimeout(int statementExecutionTimeout) {
        if (statementExecutionTimeout < 0) {
            throw new IllegalArgumentException("statementExecutionTimeout must not be negative");
        }
        this.statementExecutionTimeout = statementExecutionTimeout;
    }

    public void setStatementExecutionTimeLimitTriggeringFailoverMillis(int statementExecutionTimeLimitTriggeringFailoverMillis) {
        this.stmtExecTimeLimitTriggeringFailoverMillis = statementExecutionTimeLimitTriggeringFailoverMillis;
        this.stmtExecTimeLimitTriggeringFailoverNanos = HaDataSource.nanosInMillisecond * statementExecutionTimeLimitTriggeringFailoverMillis;
    }

    public void setStatementExecutionTimeLimitTriggeringFailbackMillis(int statementExecutionTimeLimitTriggeringFailbackMillis) {
        this.statementExecutionTimeLimitTriggeringFailbackMillis = statementExecutionTimeLimitTriggeringFailbackMillis;
        this.statementExecutionTimeLimitTriggeringFailbackNanos = HaDataSource.nanosInMillisecond * statementExecutionTimeLimitTriggeringFailbackMillis;
    }

    public void setStatementExecutionTimesIncludedInAverage(int statementExecutionTimesIncludedInAverage) {
        this.statementExecutionTimesIncludedInAverage = statementExecutionTimesIncludedInAverage;
    }

    public void setExceptionsIgnoredAfterRecovery(boolean exceptionsIgnoredAfterRecovery) {
        this.exceptionsIgnoredAfterRecovery = exceptionsIgnoredAfterRecovery;
    }

    public void setRecoveryErasesHistoryIfExceptionsIgnoredAfterRecovery(boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery) {
        this.recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery = recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery;
    }

    public void setConnectionGettingTimeout(int connectionGettingTimeout) {
        this.connectionGettingTimeout = connectionGettingTimeout;
    }
}
