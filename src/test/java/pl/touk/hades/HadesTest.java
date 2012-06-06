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
package pl.touk.hades;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.anyString;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Connection;
import java.io.*;

import pl.touk.hades.sqltimemonitoring.SqlTimeBasedTriggerImpl;
import pl.touk.hades.sqltimemonitoring.SqlTimeBasedTriggerImplTest;
import pl.touk.hades.sqltimemonitoring.SqlTimeCalculatorImpl;
import pl.touk.hades.sqltimemonitoring.SqlTimeRepo;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class HadesTest {

    @Test
    public void should() throws IOException {
        File f = File.createTempFile("foo", ".txt");
        Writer w = new OutputStreamWriter(new FileOutputStream(f), "windows-1250");
        w.write("fooąćęłńóśźżĄĆĘŁŃÓŚŹŻbar");
        w.close();
        System.out.println(f.getAbsolutePath());
    }

    @Test
    public void shouldSucceedWhenRemovingPinWhichDoesNotExist() {
        // given:
        Hades ds = new Hades();

        // when:
        ds.removePin();
        ds.removePin();

        // then:
        // OK!
    }

    @Test
    public void shouldReturnConnectionToMainDataSource() throws SQLException {
        // given:
        Hades ds = createMockFailoverDataSource(false, false);
        // when:
        String dsName = ds.getConnection().toString();
        // then:
        assertEquals("mainDs", dsName);
    }

    @Test
    public void shouldReturnConnectionToMainDataSourceWithAuth() throws SQLException {
        // given:
        Hades ds = createMockFailoverDataSource(false, false);
        // when:
        String dsName = ds.getConnection("scott", "tiger").toString();
        // then:
        assertEquals("mainDs", dsName);
    }

    @Test
    public void shouldReturnConnectionToFailoverDataSource() throws SQLException {
        // given:
        Hades ds = createMockFailoverDataSource(true, false);
        // when:
        String dsName = ds.getConnection().toString();
        // then:
        assertEquals("failoverDs", dsName);
    }

    @Test
    public void shouldReturnConnectionToFailoverDataSourceWithAuth() throws SQLException {
        // given:
        Hades ds = createMockFailoverDataSource(true, false);
        // when:
        String dsName = ds.getConnection("scott", "tiger").toString();
        // then:
        assertEquals("failoverDs", dsName);
    }

    @Test
    public void shouldThrowExceptionWhileGettingMainDs() throws SQLException {
        shouldThrowException(false, false);
    }

    @Test
    public void shouldThrowExceptionWhileGettingFailoverDs() throws SQLException {
        shouldThrowException(true, false);
    }

    @Test
    public void shouldThrowExceptionWhileGettingMainDsWihtAuth() throws SQLException {
        shouldThrowException(false, true);
    }

    @Test
    public void shouldThrowExceptionWhileGettingFailoverDsWihtAuth() throws SQLException {
        shouldThrowException(true, true);
    }

    @Test
    public void shouldLogTimeoutExceptionIfWaitingForConnectionToMainDsTooLong() throws SQLException, InterruptedException {
        Hades<SqlTimeBasedTriggerImpl> ds = new Hades<SqlTimeBasedTriggerImpl>();
        ds.setMainDataSource(createMockDataSource("mainDs", false, 1100));
        ds.setFailoverDataSource(createMockDataSource("failoverDs", false, 0));
        SqlTimeBasedTriggerImpl activator = new SqlTimeBasedTriggerImpl();
        ds.setTrigger(activator);
        SqlTimeCalculatorImpl loadCalculator = new SqlTimeCalculatorImpl();
        loadCalculator.setConnectionGettingTimeout(1);
        loadCalculator.setSqlTimeRepo(SqlTimeBasedTriggerImplTest.createSqlTimeRepoMock());
        activator.setSqlTimeCalculator(loadCalculator);
        activator.init(ds);
        activator.run();
        Thread.sleep(200);
    }

    @Test
    public void shouldLogTimeoutExceptionIfWaitingForConnectionToFailoverDsTooLong() {
        // TODO
    }

    @Test
    public void shouldPropagateSqlExceptionFromMainDsWhenTimeoutIsSet() {
        // TODO
    }

    @Test
    public void shouldPropagateSqlExceptionFromFailoverDsWhenTimeoutIsSet() {
        // TODO
    }

    @Test
    public void shouldPropagateSqlExceptionFromMainDsWhenTimeoutIsNotSet() {
        // TODO
    }

    @Test
    public void shouldPropagateSqlExceptionFromFailoverDsWhenTimeoutIsNotSet() {
        // TODO
    }

    @Test
    public void shouldNotThrowTimeoutExceptionIfWaitingForConnectionTooLongButTimeoutIsNotSet() {
        // TODO
    }

    private void shouldThrowException(boolean failover, boolean withAuth) throws SQLException {
        // given:
        Hades ds = createMockFailoverDataSource(failover, true);
        SQLException thrownException = null;
        // when:
        try {
            if (withAuth) {
                ds.getConnection("scott", "tiger");
            } else {
                ds.getConnection();
            }
        } catch (SQLException e) {
            thrownException = e;
        }
        // then:
        assertEquals(failover ? "failoverDs" : "mainDs", thrownException.getMessage());
    }

    private Hades createMockFailoverDataSource(boolean failoverActive, boolean throwException) throws SQLException {
        Hades<Trigger> ds = new Hades<Trigger>();
        ds.setMainDataSource(createMockDataSource("mainDs", throwException));
        ds.setFailoverDataSource(createMockDataSource("failoverDs", throwException));
        Trigger activator = mock(Trigger.class);
        when(activator.isFailoverActive()).thenReturn(failoverActive);
        ds.setTrigger(activator);
        return ds;
    }

    private DataSource createMockDataSource(String name, boolean throwException) throws SQLException {
        return createMockDataSource(name, throwException, 0);
    }

    private DataSource createMockDataSource(String name, boolean throwException, final int getConnectionDurationMillis) throws SQLException {
        DataSource ds = mock(DataSource.class);
        final Connection conn = mock(Connection.class);
        when(conn.toString()).thenReturn(name);
        if (!throwException) {
            when(ds.getConnection()).thenAnswer(new Answer<Connection>(){
                public Connection answer(InvocationOnMock invocationOnMock) throws Throwable {
                    Thread.sleep(getConnectionDurationMillis);
                    return conn;
                }
            });
        } else {
            when(ds.getConnection()).thenThrow(new SQLException(name));
        }
        if (!throwException) {
            when(ds.getConnection(anyString(), anyString())).thenAnswer(new Answer<Connection>(){
                public Connection answer(InvocationOnMock invocationOnMock) throws Throwable {
                    Thread.sleep(getConnectionDurationMillis);
                    return conn;
                }
            });
        } else {
            when(ds.getConnection(anyString(), anyString())).thenThrow(new SQLException(name));
        }
        return ds;
    }
}