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
package pl.touk.top.hades;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.anyString;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Connection;

import pl.touk.top.hades.load.LoadFailoverActivator;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class HaDataSourceTest {

    @Test
    public void shouldSucceedWhenRemovingPinWhichDoesNotExist() {
        // given:
        HaDataSource ds = new HaDataSource();

        // when:
        ds.removePin();
        ds.removePin();

        // then:
        // OK!
    }

    @Test
    public void shouldReturnConnectionToMainDataSource() throws SQLException {
        // given:
        HaDataSource ds = createMockFailoverDataSource(false, false);
        // when:
        String dsName = ds.getConnection().toString();
        // then:
        assertEquals("mainDs", dsName);
    }

    @Test
    public void shouldReturnConnectionToMainDataSourceWithAuth() throws SQLException {
        // given:
        HaDataSource ds = createMockFailoverDataSource(false, false);
        // when:
        String dsName = ds.getConnection("scott", "tiger").toString();
        // then:
        assertEquals("mainDs", dsName);
    }

    @Test
    public void shouldReturnConnectionToFailoverDataSource() throws SQLException {
        // given:
        HaDataSource ds = createMockFailoverDataSource(true, false);
        // when:
        String dsName = ds.getConnection().toString();
        // then:
        assertEquals("failoverDs", dsName);
    }

    @Test
    public void shouldReturnConnectionToFailoverDataSourceWithAuth() throws SQLException {
        // given:
        HaDataSource ds = createMockFailoverDataSource(true, false);
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
    public void shouldLogTimeoutExceptionIfWaitingForConnectionToMainDsTooLong() throws SQLException {
        HaDataSource<LoadFailoverActivator> ds = new HaDataSource<LoadFailoverActivator>();
        ds.setMainDataSource(createMockDataSource("mainDs", false, 2000));
        ds.setFailoverDataSource(createMockDataSource("failoverDs", false, 0));
        LoadFailoverActivator activator = new LoadFailoverActivator();
        ds.setFailoverActivator(activator);
        activator.init(ds);
        activator.setConnectionGettingTimeout(1);
        activator.run();
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
        HaDataSource ds = createMockFailoverDataSource(failover, true);
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

    private HaDataSource createMockFailoverDataSource(boolean failoverActive, boolean throwException) throws SQLException {
        HaDataSource<FailoverActivator> ds = new HaDataSource<FailoverActivator>();
        ds.setMainDataSource(createMockDataSource("mainDs", throwException));
        ds.setFailoverDataSource(createMockDataSource("failoverDs", throwException));
        FailoverActivator activator = mock(FailoverActivator.class);
        when(activator.isFailoverActive()).thenReturn(failoverActive);
        ds.setFailoverActivator(activator);
        return ds;
    }

    private DataSource createMockDataSource(String name, boolean throwException) throws SQLException {
        return createMockDataSource(name, throwException, 0);
    }

    private DataSource createMockDataSource(String name, boolean throwException, final int getConnectionDurationMillis) throws SQLException {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        when(conn.toString()).thenReturn(name);
        if (!throwException) {
            when(ds.getConnection()).thenReturn(conn);
        } else {
            when(ds.getConnection()).thenThrow(new SQLException(name));
        }
        if (!throwException) {
            when(ds.getConnection(anyString(), anyString())).thenReturn(conn);
        } else {
            when(ds.getConnection(anyString(), anyString())).thenThrow(new SQLException(name));
        }
        return ds;
    }
}