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
package pl.touk.hades.sql.timemonitoring;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import pl.touk.hades.Hades;
import pl.touk.hades.sql.exception.ConnException;

import javax.sql.DataSource;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Timer;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class SqlTimeBasedTimerTaskMonitorTest {

    @Test
    public void shouldCancelExecutionScheduledAtFixedRateIfLastExecutionDelayedTheCurrentOne()
            throws InterruptedException, SQLException, ConnException, Hades.NoMonitorException, UnknownHostException {
        shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(true);
    }

    @Test
    public void shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne()
            throws InterruptedException, SQLException, ConnException, Hades.NoMonitorException, UnknownHostException {
        shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(false);
    }

    private void shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(boolean scheduleAtFixedRate)
            throws InterruptedException, SQLException, ConnException, Hades.NoMonitorException, UnknownHostException {
        // given:
        final float[] runMethodExecutionCount = new float[]{0};
        int periodMillis = 200;
        int sleepTimeEqualsToThreeAndAHalfPeriodsMillis = (int) (3.5 * periodMillis);
        int runMethodExecTimeEqualsToOneAndAHalfPeriodsMillis = (int) (1.5 * periodMillis);
        SqlTimeBasedTimerTaskMonitor monitor = createSqlTimeBasedTimerTaskMonitorExecutingRunMethodForGivenTimeMillis(
                runMethodExecTimeEqualsToOneAndAHalfPeriodsMillis,
                runMethodExecutionCount);

        // when:
        Timer timer = new Timer();
        if (scheduleAtFixedRate) {
            timer.scheduleAtFixedRate(monitor, 0, periodMillis);
        } else {
            timer.schedule(monitor, 0, periodMillis);
        }
        Thread.sleep(sleepTimeEqualsToThreeAndAHalfPeriodsMillis);
        timer.cancel();

        // then:
        assertEquals(2, (int) runMethodExecutionCount[0]);
    }

    @Test
    public void shouldFailover() throws SQLException, InterruptedException, UnknownHostException {
        // given:
        SqlTimeBasedTimerTaskMonitor activator = createSqlTimeBasedTimerTaskMonitor(200, 100,
                new int[]{
                        33, 66, 50, 50, 50, 150, 133, 166, 150, 250, 250, 250, 250, 300,
                        150, 166, 133, 150, 50, 250, 50, 300, 50, 250, 150, 250, 250},
                new int[]{
                        66, 33, 50, 150, 250, 50, 166, 133, 250, 300, 50, 50, 150, 250,
                        50, 133, 166, 150, 50, 150, 150, 250, 250, 50, 250, 150, 350});
        boolean[] expectedFailoverActive = new boolean[]{
                false, false, false, false, false, false, false, false, false, false, true, true, true,
                true, true, true, true, true, false, true, false, true, false, true, false, true, false};
        boolean[] actualFailoverActive = new boolean[expectedFailoverActive.length];

        // when:
        for (int i = 0; i < expectedFailoverActive.length; i++) {
            activator.run();
            actualFailoverActive[i] = activator.isFailoverActive();
        }

        // then:
        for (int i = 0; i < expectedFailoverActive.length; i++) {
            assertEquals("i: " + i, expectedFailoverActive[i], actualFailoverActive[i]);
        }
    }

    @Test
    public void shouldFailoverForEqualLimits() throws SQLException, InterruptedException, UnknownHostException {
        // given:
        SqlTimeBasedTimerTaskMonitor activator = createSqlTimeBasedTimerTaskMonitor(200, 200,
                new int[]{
                        33, 66, 50, 50, 50, 150, 133, 166, 150, 250, 250, 250,
                        250, 300, 50, 250, 50, 300, 50, 250, 150, 250, 250},
                new int[]{
                        66, 33, 50, 150, 250, 50, 166, 133, 250, 300, 50, 50,
                        150, 250, 50, 150, 150, 250, 250, 50, 250, 150, 350});
        boolean[] expectedFailoverActive = new boolean[]{
                false, false, false, false, false, false, false, false, false, false, true,
                true, true, true, false, true, false, true, false, true, false, true, false};
        boolean[] actualFailoverActive = new boolean[expectedFailoverActive.length];

        // when:
        for (int i = 0; i < expectedFailoverActive.length; i++) {
            activator.run();
            actualFailoverActive[i] = activator.isFailoverActive();
        }

        // then:
        for (int i = 0; i < expectedFailoverActive.length; i++) {
            assertEquals("i: " + i, expectedFailoverActive[i], actualFailoverActive[i]);
        }
    }

    private SqlTimeBasedTimerTaskMonitor createSqlTimeBasedTimerTaskMonitorExecutingRunMethodForGivenTimeMillis(
            final int runMethodExecutionTimeMillis,
            final float[] runMethodExecutionCount)
            throws SQLException, ConnException, InterruptedException, Hades.NoMonitorException, UnknownHostException {
        Hades<SqlTimeBasedMonitorImpl> hades = mock(Hades.class);
        when(hades.getDsName(eq(true), anyBoolean())).thenReturn("FAILOVER_DB");
        when(hades.getDsName(eq(false), anyBoolean())).thenReturn("MAIN_DB    ");

        RepoJdbcImpl sqlTimeRepoMock = mock(RepoJdbcImpl.class);
        when(sqlTimeRepoMock.findSqlTimeYoungerThan(
                any(MonitorRunLogPrefix.class),
                anyString(),
                anyString())).thenReturn(null);
        SqlTimeCalculatorImpl calc = new SqlTimeCalculatorImpl(
                hades,
                sqlTimeRepoMock,
                // one thread to be sure that main and failover sql times are measured sequentially:
                Executors.newFixedThreadPool(1)
        );

        Repo repo = createSqlTimeRepoMock();
        SqlTimeBasedTimerTaskMonitor monitor =
                new SqlTimeBasedTimerTaskMonitor(hades, 100, 50, 1, true, true, calc, 1, 1, 1, repo);

        Connection connectionMock = mock(Connection.class);
        PreparedStatement statementMock = mock(PreparedStatement.class);
        when(hades.getConnection(any(MonitorRunLogPrefix.class), anyBoolean())).thenReturn(connectionMock);
        when(hades.getFailoverDsName()).thenReturn("FAILOVER_DS");
        when(hades.getMainDsName()).thenReturn("MAIN_DS");
        when(hades.getFailoverDataSourcePinned()).thenReturn(null);
//        when(hades.getMonitor()).thenReturn(monitor);
        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(statementMock.execute()).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                runMethodExecutionCount[0] += 0.5;
                Thread.sleep(runMethodExecutionTimeMillis / 2);
                return null;
            }
        });

        monitor.init();

        return monitor;
    }

    private SqlTimeBasedTimerTaskMonitor createSqlTimeBasedTimerTaskMonitor(int sqlExecTimeTriggeringFailoverMillis,
                                                                            int sqlExecTimeTriggeringFailbackMillis,
                                                                            final int[] mainDbExecutionTimesMillis,
                                                                            final int[] failoverDbExecutionTimesMillis)
            throws SQLException, InterruptedException, UnknownHostException {
        Hades<SqlTimeBasedMonitorImpl> hades = new Hades<SqlTimeBasedMonitorImpl>(
                createDsMock(mainDbExecutionTimesMillis),
                createDsMock(failoverDbExecutionTimesMillis),
                "MAIN_DS",
                "FAILOVER_DS");

        Repo repo = createSqlTimeRepoMock();
        SqlTimeCalculatorImpl sqlTimeCalculator = new SqlTimeCalculatorImpl(hades, repo, null);

        SqlTimeBasedTimerTaskMonitor monitor = new SqlTimeBasedTimerTaskMonitor(
                hades,
                sqlExecTimeTriggeringFailoverMillis,
                sqlExecTimeTriggeringFailbackMillis,
                1,
                true,
                true,
                sqlTimeCalculator,
                1,
                1,
                1,
                repo);

        monitor.init();

        return monitor;
    }

    public static Repo createSqlTimeRepoMock() throws InterruptedException {
        Repo repo = mock(RepoJdbcImpl.class);
        when(repo.findSqlTimeYoungerThan(any(MonitorRunLogPrefix.class), anyString(), anyString())).thenReturn(null);
        when(repo.getHost()).thenReturn("localhost");
        when(repo.getRepoId()).thenReturn("repo1");
        final int indexOfSqlTimeToStoreParameter = 3;
        when(repo.storeSqlTime(
                any(MonitorRunLogPrefix.class),
                any(Hades.class),
                anyString(),
                anyLong(),
                anyString())).thenAnswer(new Answer<Long>() {
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                return (Long) invocationOnMock.getArguments()[indexOfSqlTimeToStoreParameter];
            }
        });
        when(repo.storeException(
                any(MonitorRunLogPrefix.class),
                any(Hades.class),
                anyString(),
                any(Exception.class),
                anyString())).thenCallRealMethod();
        return repo;
    }

    private DataSource createDsMock(final int[] executionTimesMillis) throws SQLException {
        DataSource dsMock = mock(DataSource.class);
        Connection connectionMock = mock(Connection.class);
        PreparedStatement statementMock = mock(PreparedStatement.class);

        when(dsMock.getConnection()).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(statementMock.execute()).thenAnswer(new Answer() {
            private int i = 0;
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(executionTimesMillis[i]);
                i++;
                return null;
            }
        });

        return dsMock;
    }
}
