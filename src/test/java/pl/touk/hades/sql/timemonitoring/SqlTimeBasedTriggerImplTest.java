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
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;

import javax.sql.DataSource;
import java.util.Timer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import pl.touk.hades.Hades;
import pl.touk.hades.sql.exception.ConnException;
import pl.touk.hades.sql.timemonitoring.repo.SqlTimeRepo;

/**
* @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
*/
public class SqlTimeBasedTriggerImplTest {

    @Test
    public void shouldCancelExecutionScheduledAtFixedRateIfLastExecutionDelayedTheCurrentOne() throws InterruptedException, SQLException, ConnException, Hades.NoTriggerException {
        shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(true);
    }

    @Test
    public void shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne() throws InterruptedException, SQLException, ConnException, Hades.NoTriggerException {
        shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(false);
    }

    private void shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(boolean scheduleAtFixedRate) throws InterruptedException, SQLException, ConnException, Hades.NoTriggerException {
        // given:
        final float[] runMethodExecutionCount = new float[]{0};
        int periodMillis = 200;
        int sleepTimeEqualsToThreeAndAHalfPeriodsMillis = (int) (3.5 * periodMillis);
        int runMethodExecTimeEqualsToOneAndAHalfPeriodsMillis = (int) (1.5 * periodMillis);
        SqlTimeBasedTriggerImpl activator = createFailoverActivatorImplExecutingRunMethodForGivenTimeMillis(runMethodExecTimeEqualsToOneAndAHalfPeriodsMillis, runMethodExecutionCount);

        // when:
        Timer timer = new Timer();
        if (scheduleAtFixedRate) {
            timer.scheduleAtFixedRate(activator, 0, periodMillis);
        } else {
            timer.schedule(activator, 0, periodMillis);
        }
        Thread.sleep(sleepTimeEqualsToThreeAndAHalfPeriodsMillis);
        timer.cancel();

        // then:
        assertEquals(2, (int) runMethodExecutionCount[0]);
    }

    @Test
    public void shouldFailover() throws SQLException, InterruptedException {
        // given:
        SqlTimeBasedTriggerImpl activator = createSqlTimeBasedTriggerImpl(200, 100,
                new int[]{33, 66, 50, 50, 50, 150, 133, 166, 150, 250, 250, 250, 250, 300, 150, 166, 133, 150, 50, 250, 50, 300, 50, 250, 150, 250, 250},
                new int[]{66, 33, 50, 150, 250, 50, 166, 133, 250, 300, 50, 50, 150, 250, 50, 133, 166, 150, 50, 150, 150, 250, 250, 50, 250, 150, 350});
        boolean[] expectedFailoverActive = new boolean[]
                         {false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, false, true, false, true, false, true, false, true, false};
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
    public void shouldFailoverForEqualLimits() throws SQLException, InterruptedException {
        // given:
        SqlTimeBasedTriggerImpl activator = createSqlTimeBasedTriggerImpl(200, 200,
                new int[]{33, 66, 50, 50, 50, 150, 133, 166, 150, 250, 250, 250, 250, 300, 50, 250, 50, 300, 50, 250, 150, 250, 250},
                new int[]{66, 33, 50, 150, 250, 50, 166, 133, 250, 300, 50, 50, 150, 250, 50, 150, 150, 250, 250, 50, 250, 150, 350});
        boolean[] expectedFailoverActive = new boolean[]
                         {false, false, false, false, false, false, false, false, false, false, true, true, true, true, false, true, false, true, false, true, false, true, false};
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

    private SqlTimeBasedTriggerImpl createFailoverActivatorImplExecutingRunMethodForGivenTimeMillis(final int runMethodExecutionTimeMillis, final float[] runMethodExecutionCount) throws SQLException, ConnException, InterruptedException, Hades.NoTriggerException {
        Hades hadesMock = mock(Hades.class);

        SqlTimeCalculatorImpl sqlTimeCalculator = new SqlTimeCalculatorImpl();
        SqlTimeRepo sqlTimeRepoMock = mock(SqlTimeRepo.class);
        when(sqlTimeRepoMock.findSqlTimeYoungerThan(anyString(), anyString())).thenReturn(null);
        sqlTimeCalculator.setSqlTimeRepo(sqlTimeRepoMock);

        SqlTimeBasedTriggerImpl trigger = new SqlTimeBasedTriggerImpl(hadesMock, 100, 50, 1, true, true, sqlTimeCalculator, 1, 1, 1);

        Connection connectionMock = mock(Connection.class);
        PreparedStatement statementMock = mock(PreparedStatement.class);
        when(hadesMock.getConnection(anyString(), anyBoolean())).thenReturn(connectionMock);
        when(hadesMock.getFailoverDsName()).thenReturn("FAILOVER_DS");
        when(hadesMock.getMainDsName()).thenReturn("MAIN_DS");
        when(hadesMock.getFailoverDataSourcePinned()).thenReturn(null);
        when(hadesMock.getTrigger()).thenReturn(trigger);
        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(statementMock.execute()).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                runMethodExecutionCount[0] += 0.5;
                Thread.sleep(runMethodExecutionTimeMillis / 2);
                return null;
            }
        });

        trigger.init();

        return trigger;
    }

    private SqlTimeBasedTriggerImpl createSqlTimeBasedTriggerImpl(int statementExecutionTimeLimitTriggeringFailoverMillis, int statementExecutionTimeLimitTriggeringFailbackMillis, final int[] mainDbExecutionTimesMillis, final int[] failoverDbExecutionTimesMillis) throws SQLException, InterruptedException {
        Hades<SqlTimeBasedTriggerImpl> hades = new Hades<SqlTimeBasedTriggerImpl>(createDsMock(mainDbExecutionTimesMillis), createDsMock(failoverDbExecutionTimesMillis), "MAIN_DS", "FAILOVER_DS");

        SqlTimeCalculatorImpl sqlTimeCalculator = new SqlTimeCalculatorImpl();
        sqlTimeCalculator.setSqlTimeRepo(createSqlTimeRepoMock());

        SqlTimeBasedTriggerImpl trigger = new SqlTimeBasedTriggerImpl(hades,
                statementExecutionTimeLimitTriggeringFailoverMillis,
                statementExecutionTimeLimitTriggeringFailbackMillis,
                1,
                true,
                true,
                sqlTimeCalculator,
                1,
                1,
                1);

        trigger.init();

        return trigger;
    }

    public static SqlTimeRepo createSqlTimeRepoMock() throws InterruptedException {
        SqlTimeRepo sqlTimeRepoMock = mock(SqlTimeRepo.class);
        when(sqlTimeRepoMock.findSqlTimeYoungerThan(anyString(), anyString())).thenReturn(null);
        when(sqlTimeRepoMock.storeSqlTime(anyString(), anyString(), anyLong())).thenAnswer(new Answer<Long>() {
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                return (Long) invocationOnMock.getArguments()[2];
            }
        });
        return sqlTimeRepoMock;
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
