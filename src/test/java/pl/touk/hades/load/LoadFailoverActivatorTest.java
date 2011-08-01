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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyBoolean;

import javax.sql.DataSource;
import java.util.Timer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import pl.touk.hades.HaDataSource;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class LoadFailoverActivatorTest {

    @Test
    public void shouldCancelExecutionScheduledAtFixedRateIfLastExecutionDelayedTheCurrentOne() throws InterruptedException, SQLException {
        shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(true);
    }

    @Test
    public void shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne() throws InterruptedException, SQLException {
        shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(false);
    }

    private void shouldCancelExecutionIfLastExecutionDelayedTheCurrentOne(boolean scheduleAtFixedRate) throws InterruptedException, SQLException {
        // given:
        final float[] runMethodExecutionCount = new float[]{0};
        int periodMillis = 200;
        int sleepTimeEqualsToThreeAndAHalfPeriodsMillis = (int) (3.5 * periodMillis);
        int runMethodExecTimeEqualsToOneAndAHalfPeriodsMillis = (int) (1.5 * periodMillis);
        LoadFailoverActivator activator = createFailoverActivatorImplExecutingRunMethodForGivenTimeMillis(runMethodExecTimeEqualsToOneAndAHalfPeriodsMillis, runMethodExecutionCount);

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
    public void shouldFailover() throws SQLException {
        // given:
        LoadFailoverActivator activator = createFailoverActivatorImpl(200, 100,
                new int[]{33,    66,    50,    50,    50,    150,   133,   166,   150,   250,   250,  250,  250,  300,  150,  166,  133,  150,  50,    250,  50,    300,  50,    250,  150,   250,  250},
                new int[]{66,    33,    50,    150,   250,   50,    166,   133,   250,   300,   50,   50,   150,  250,  50,   133,  166,  150,  50,    150,  150,   250,  250,   50,   250,   150,  350});
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
    public void shouldFailoverForEqualLimits() throws SQLException {
        // given:
        LoadFailoverActivator activator = createFailoverActivatorImpl(200, 200,
                new int[]{33,    66,    50,    50,    50,    150,   133,   166,   150,   250,   250,  250,  250,  300,  50,    250,  50,    300,  50,    250,  150,   250,  250},
                new int[]{66,    33,    50,    150,   250,   50,    166,   133,   250,   300,   50,   50,   150,  250,  50,    150,  150,   250,  250,   50,   250,   150,  350});
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

    private LoadFailoverActivator createFailoverActivatorImplExecutingRunMethodForGivenTimeMillis(final int runMethodExecutionTimeMillis, final float[] runMethodExecutionCount) throws SQLException {
        LoadFailoverActivator activator = new LoadFailoverActivator();
        HaDataSource haDataSourceMock = mock(HaDataSource.class);
        Connection connectionMock = mock(Connection.class);
        PreparedStatement statementMock = mock(PreparedStatement.class);
        when(haDataSourceMock.getConnection(anyBoolean(), anyBoolean(), anyString(), anyString(), anyString())).thenReturn(connectionMock);
        when(haDataSourceMock.getFailoverDataSourceName()).thenReturn("FAILOVER_DS");
        when(haDataSourceMock.getMainDataSourceName()).thenReturn("MAIN_DS");
        when(haDataSourceMock.getFailoverDataSourcePinned()).thenReturn(null);
        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(statementMock.execute()).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                runMethodExecutionCount[0] += 0.5;
                Thread.sleep(runMethodExecutionTimeMillis / 2);
                return null;
            }
        });
        activator.init(haDataSourceMock);
        return activator;
    }

    private LoadFailoverActivator createFailoverActivatorImpl(int statementExecutionTimeLimitTriggeringFailoverMillis, int statementExecutionTimeLimitTriggeringFailbackMillis, final int[] mainDbExecutionTimesMillis, final int[] failoverDbExecutionTimesMillis) throws SQLException {
        LoadFailoverActivator activator = new LoadFailoverActivator();
        HaDataSource haDataSource = new HaDataSource();
        DataSource enclosedMainDataSourceMock = createDsMock(mainDbExecutionTimesMillis);
        DataSource enclosedFailoverDataSourceMock = createDsMock(failoverDbExecutionTimesMillis);
        haDataSource.setFailoverDataSource(enclosedFailoverDataSourceMock);
        haDataSource.setMainDataSource(enclosedMainDataSourceMock);
        haDataSource.setFailoverDataSourceName("FAILOVER_DS");
        haDataSource.setMainDataSourceName("MAIN_DS");
        activator.setStatementExecutionTimeLimitTriggeringFailoverMillis(statementExecutionTimeLimitTriggeringFailoverMillis);
        activator.setStatementExecutionTimeLimitTriggeringFailbackMillis(statementExecutionTimeLimitTriggeringFailbackMillis);
        activator.init(haDataSource);
        return activator;
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
