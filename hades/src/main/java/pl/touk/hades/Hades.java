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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.sql.exception.ConnException;

/**
 * A data source which offers high-availability (HA) by enclosing two real data sources - the main one and the failover
 * one. Which enclosed data source is used is not determined by the HA data source itself. It is the responibility of
 * {@link Monitor} associated with this HA data source.
 * <p>
 * This class should be used as follows:
 * <pre>
 * DataSource mainDs = new SomeDataSourceImplementation(...);
 * DataSource failoverDs = new SomeOtherDataSourceImplementation(...);
 *
 * FailoverActivator someImplementationOfFailoverActivator = new LoadFailoverActivator();
 * // Additional configuration of the above activator goes here. For example:
 * someImplementationOfFailoverActivator.setStatement('select * from SOME_TABLE'));
 *
 * HaDataSource haDataSource = new HaDataSource();
 * haDataSource.setMainDataSource(mainDs);
 * haDataSource.setFailoverDataSource(failoverDs);
 *
 * // The following two calls associate the HA data source with the failover activator:
 * haDataSource.setFailoverActivator(someImplementationOfFailoverActivator);
 * haDataSource.init();
 *
 * // The HA data source is ready to be used. Now the activator must be invoked, for
 * // example periodicaly in a separate thread, to control the switching between the
 * // main and the failover data sources. For example every 20 seconds:
 * new Timer().schedule(someImplementationOfFailoverActivator, 0, 20000);
 *
 * // Now HA data source is controlled by the activator. If the activator decides that
 * // the failover is necessary (because for example mainDs.getConnection() throws an
 * // exception) it will silently do it. After the failover, connections from the failover
 * // data source are returned. If the activator decides that the failback is possible
 * // (because for example mainDs.getConnection() starts to retun connections) it can
 * // silently do it. After the failback, connections from the main data source are returned.
 *
 * // Here go some operations. As some time passes a failover can be activated
 * // (by someImplementationOfFailoverActivator in the above timer thread) if
 * // something is wrong with the main data source.
 * ...
 *
 * conn1 = haDataSource().getConnection(); // conn1 can be a connection from failover data
 *                                         // source if failover was activated by the above timer
 *                                         // someImplementationOfFailoverActivator.
 * // Here go some operations on conn1.
 * ...
 *
 * // Here go some operations. As some time passes a failback can be activated
 * // (by someImplementationOfFailoverActivator in the above timer thread) if
 * // the main data source is back to normal.
 * ...
 *
 * conn2 = haDataSource().getConnection(); // conn2 can be a connection from main data
 *                                         // source if failback was activated by
 *                                         // someImplementationOfFailoverActivator.
 * // Here go some operations on conn2.
 * ...
 * </pre>
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class Hades<M extends Monitor> implements DataSource, HadesMBean {

    private final static Logger logger = LoggerFactory.getLogger(Hades.class);

    private final static String errorMsg = "this class wraps two data sources so this method is irrelevant as it does not give the possbility to specify which data source should it operate on";

    private final static int notPinned = 0;
    private final static int mainPinned = 1;
    private final static int failoverPinned = 2;

    private final DataSource mainDataSource;
    private final DataSource failoverDataSource;
    private final String mainDsName;
    private final String failoverDsName;
    private final String mainDsNameFixedWidth;
    private final String failoverDsNameFixedWidth;
    private final AtomicReference<M> monitor;
    private final AtomicInteger failoverDataSourcePinned;

    public Hades(DataSource mainDataSource, DataSource failoverDataSource, String mainDsName, String failoverDsName) {
        Utils.assertNotNull(mainDataSource, "mainDataSource");
        Utils.assertNotNull(failoverDataSource, "failoverDataSource");
        Utils.assertNonEmpty(mainDsName, "mainDsName");
        Utils.assertNonEmpty(failoverDsName, "failoverDsName");

        this.mainDataSource = mainDataSource;
        this.failoverDataSource = failoverDataSource;
        this.mainDsName = mainDsName;
        this.failoverDsName = failoverDsName;

        int width = Math.max(mainDsName.length(), failoverDsName.length());
        this.mainDsNameFixedWidth = appendSpaces(mainDsName, width);
        this.failoverDsNameFixedWidth = appendSpaces(failoverDsName, width);

        this.monitor = new AtomicReference<M>();
        this.failoverDataSourcePinned = new AtomicInteger(notPinned);
    }

    public void init(M monitor) {
        Utils.assertNotNull(monitor, "monitor");
        Utils.assertSame(monitor.getHades(), this, "monitor.hades != this; ensure that each association of a hades and a monitor is one-to-one");
        if (!this.monitor.compareAndSet(null, monitor)) {
            throw new IllegalStateException("hades already associated with a monitor");
        }
    }

    private String appendSpaces(String s, int width) {
        StringBuilder sb = new StringBuilder(s != null ? s : "");
        for (int i = sb.length(); i < width; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    /**
     * Invokes {@link javax.sql.DataSource#getConnection() getConnection()} on the main data source or on the failover
     * data source if failover is inactive or active respectively.
     * To check whether failover is active {@link Monitor#isFailoverActive() isFailoverActive()} on the
     * associated failover activator is invoked.
     * <p>
     * The above check is ommited if the main data source is pinned (see {@link #pinMainDataSource()}).
     * In such case connections from the main data source are returned.
     * <p>
     * The above check is also ommited if the failover data source is pinned (see {@link #pinFailoverDataSource()}).
     * In such case connections from the failover data source are returned.
     *
     * @return connection from the main data source if failover is inactive or from the failover data source otherwise
     * @throws SQLException if getting a connection from one of the two enclosed data sources throws an exception
     */
    public Connection getConnection() throws SQLException, RuntimeException {
        return getConnection("", failoverEffectivelyActive(), false, null, null, true);
    }

    /**
     * Invokes {@link javax.sql.DataSource#getConnection(String, String) getConnection(username, password)} on the main
     * data source or on the failover data source if failover is inactive or active respectively.
     * To check whether failover is active {@link Monitor#isFailoverActive() isFailoverActive()} on the
     * associated failover activator is invoked.
     * <p>
     * The above check is ommited if the main data source is pinned (see {@link #pinMainDataSource()}).
     * In such case connections from the main data source are returned.
     * <p>
     * The above check is also ommited if the failover data source is pinned (see {@link #pinFailoverDataSource()}).
     * In such case connections from the failover data source are returned.
     *
     * @param username username for which the connection should be returned
     * @param password password for the specified <code>username</code>
     * @return connection from the main data source if failover is inactive or from the failover data source otherwise
     * @throws SQLException if getting a connection from one of the two enclosed data sources throws an exception
     */
    public Connection getConnection(String username, String password) throws SQLException, RuntimeException {
        return getConnection("", failoverEffectivelyActive(), true, username, password, true);
    }

    /**
     * Delegates to {@link #getConnection(String, boolean, boolean, String, String, boolean)} simply surrounding possible
     * thrown exceptions by {@link pl.touk.hades.sql.exception.ConnException}.
     *
     *
     * @param logPrefix text used as a log prefix
     * @param failover whether to return a connection from the failover data source or from the main data source
     * @return connection from the specified data source
     * @throws pl.touk.hades.sql.exception.ConnException if the specified data source throws the runtime exception
     */
    public Connection getConnection(String logPrefix, boolean failover) throws ConnException {
        try {
            return getConnection(logPrefix, failover, false, null, null, false);
        } catch (Exception e) {
            throw new ConnException(logPrefix, e);
        }
    }

    /**
     * Delegates to {@link javax.sql.DataSource#getConnection() getConnection()} (if <code>withAuth</code> is
     * <code>false</code>) or to {@link javax.sql.DataSource#getConnection(String, String) getConnection(username, password)}
     * (if <code>withAuth</code> is <code>true</code>). The time of the above invocation is measured and logged in
     * debug mode (the log is prefixed with <code>logPrefix</code> parameter) or error mode if the invocation ends
     * with an exception.
     * <p>
     * This method was created for those failover activators that need to get connections from the main data source
     * and the failover data source to decide whether failover should be active or not.
     *
     *
     *
     * @param logPrefix text used as a log prefix
     * @param failover whether to return a connection from the failover data source or from the main data source
     * @param withAuth whether to delegate to {@link javax.sql.DataSource#getConnection(String, String)} or to {@link javax.sql.DataSource#getConnection()}
     * @param username username for which the connection should be returned (ignored if <code>withAuth</code> is <code>false</code>)
     * @param password password for the given <code>username</code> (ignored if <code>withAuth</code> is <code>false</code>)
     * @param informMonitor d
     * @return connection from the specified data source
     * @throws SQLException if the specified data source throws the exception
     * @throws RuntimeException if the specified data source throws the runtime exception
     */
    private Connection getConnection(String logPrefix,
                                     boolean failover,
                                     boolean withAuth,
                                     String username,
                                     String password,
                                     boolean informMonitor) throws SQLException, RuntimeException {
        DataSource ds = failover ? failoverDataSource : mainDataSource;
        Connection connection;
        long start = System.nanoTime();
        try {
            connection = withAuth ? ds.getConnection(username, password) : ds.getConnection();
        } catch (SQLException e) {
            throw handleException(logPrefix, e, start, connDesc(withAuth, username, failover), failover, informMonitor);
        } catch (RuntimeException e) {
            throw handleException(logPrefix, e, start, connDesc(withAuth, username, failover), failover, informMonitor);
        }
        try {
            long timeElapsedNanos = System.nanoTime() - start;
            if (informMonitor) {
                connectionRequested(true, failover, timeElapsedNanos);
            }
            if (logger.isDebugEnabled()) {
                logger.debug(logPrefix + "successfully got" + connDesc(withAuth, username, failover) + "in " + Utils.nanosToMillisAsStr(timeElapsedNanos));
            }
            return connection;
        } catch (RuntimeException e) {
            try {
                connection.close();
            } catch (SQLException e1) {
                logger.error(logPrefix + "successfully got" + connDesc(withAuth, username, failover) + "but unexpected exception occurred (logged below); after that an attempt to close the connection resulted in another exception", e1);
            } finally {
                logger.error(logPrefix + "successfully got" + connDesc(withAuth, username, failover) + "but unexpected exception occurred", e);
            }
            throw e;
        }
    }

    private String connDesc(boolean withAuth, String username, boolean failover) {
        return  " a connection" + (withAuth ? " for username " + username : "") + " to " + (failover ? failoverDsName + " (failover" : mainDsName + " (main") + " ds) ";
    }

    private <T extends Throwable> T handleException(String logPrefix, T e, long start, String connDesc, boolean failover, boolean informMonitor) throws T {
        long timeElapsedNanos = System.nanoTime() - start;
        if (informMonitor) {
            connectionRequested(false, failover, timeElapsedNanos);
        }
        logger.error(logPrefix + "exception while getting " + connDesc + "caught in " + Utils.nanosToMillisAsStr(timeElapsedNanos), e);
        return e;
    }

    private void connectionRequested(boolean success, boolean failover, long timeElapsedNanos) {
        try {
            getMonitor().connectionRequestedFromHades(success, failover, timeElapsedNanos);
        } catch (NoMonitorException e) {
        }
    }

    @Override
    public String toString() {
        return mainDsName + "/" + failoverDsName;
    }

    public String desc(boolean failover) {
        return (failover ? getFailoverDsName() + " (failover" : getMainDsName() + " (main") + " ds)";
    }

    private boolean failoverEffectivelyActive() {
        Boolean b = getFailoverDataSourcePinned();
        if (b == null) {
            try {
                return getMonitor().isFailoverActive();
            } catch (NoMonitorException e) {
                logger.warn(e.getMessage(), e);
                return false;
            }
        } else {
            return b;
        }
    }

    /**
     * Pins the main data source. From now on methods {@link #getConnection()} and {@link #getConnection(String, String)}
     * will get connections from the main data source.
     */
    public void pinMainDataSource() {
        failoverDataSourcePinned.set(mainPinned);
    }

    /**
     * Pins the failover data source. From now on methods {@link #getConnection()} and {@link #getConnection(String, String)}
     * will get connections from the failover data source.
     */
    public void pinFailoverDataSource() {
        failoverDataSourcePinned.set(failoverPinned);
    }

    /**
     * Removes pin enabled by {@link #pinMainDataSource()} or {@link #pinFailoverDataSource()}. From now on methods
     * {@link #getConnection()} and {@link #getConnection(String, String)} will ask the failover activator whether
     * failover is active.
     */
    public void removePin() {
        failoverDataSourcePinned.set(notPinned);
    }

    /**
     * Returnes <code>true</code> if pin is enabled and <code>false</code> otherwise. The pin can be enabled by
     * {@link #pinMainDataSource()} or {@link #pinFailoverDataSource()}. The pin can be disabled by
     * {@link #removePin()}.
     *
     * @return <code>true</code> if pin is enabled and <code>false</code> otherwise
     */
    public boolean getPinEnabled() {
        return failoverDataSourcePinned.get() != notPinned;
    }

    /**
     * Returns the pin status of this HA data source. Pin status is described as follows. If no pin is
     * enabled <code>null</code> is returned. If the main data source is pinned (see {@link #pinMainDataSource()}
     * <code>true</code> is returned. If the failover data source is pinned (see {@link #pinFailoverDataSource()}
     * <code>false</code> is returned.
     *
     * @return the pin status of this HA data source
     */
    public Boolean getFailoverDataSourcePinned() {
        int i = failoverDataSourcePinned.get();
        if (notPinned == i) {
            return null;
        } else {
            return failoverPinned == i;
        }
    }

    /**
     * Returns <code>true</code> if the failover data source is used and <code>false</code> if the main data source is used.
     * The failover data source is used (i.e. {@link #getConnection()} and {@link #getConnection(String, String)}
     * returns connections from it) if it is pinned (see {@link #pinFailoverDataSource()}). Similarly, the main data
     * source is used if it is pinned (see {@link #pinMainDataSource()}). If no pin is enabled and
     * {@link #getMonitor()}.{@link Monitor#isFailoverActive() isFailoverActive()} returns
     * <code>false</code> the main data source is used.
     * If no pin is enabled and
     * {@link #getMonitor ()}.{@link Monitor#isFailoverActive() isFailoverActive()} returns
     * <code>true</code> the failover data source is used.
     *
     * @return <code>true</code> if the failover data source is used, <code>false</code> otherwise
     */
    public boolean isFailoverActive() {
        return failoverEffectivelyActive();
    }

    public String getActiveDataSourceName() {
        return failoverEffectivelyActive() ? getFailoverDsName() : getMainDsName();
    }


    public String getFailoverLoad() {
        return getLoadLevel(true);
    }

    public String getMainLoad() {
        return getLoadLevel(false);
    }

    private String getLoadLevel(boolean failover) {
        try {
            return getMonitor().getLoad().getLoadLevel(failover).name();
        } catch (NoMonitorException e) {
            return e.getMessage();
        }
    }

    /**
     * Throws <code>UnsupportedOperationException</code>.
     *
     * @throws UnsupportedOperationException always
     */
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException(errorMsg);
    }

    /**
     * Throws <code>UnsupportedOperationException</code>.
     *
     * @throws UnsupportedOperationException always
     */
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException(errorMsg);
    }

    /**
     * Sets the given login timeout on the main data source and on the failover data source.
     *
     * @param loginTimeout login timeout that should be set
     * @throws SQLException if setting timeout on any of the two enclosed data sources throws the exception
     */
    public void setLoginTimeout(int loginTimeout) throws SQLException {
        mainDataSource.setLoginTimeout(loginTimeout);
        failoverDataSource.setLoginTimeout(loginTimeout);
    }

    /**
     * Sets the given print writer as the log writer on the main data source and on the failover data source.
     *
     * @param printWriter print writer that should be set as the log writer
     * @throws SQLException if setting the given print writer as the log writer on any of the two enclosed data sources
     * throws the exception
     */
    public void setLogWriter(PrintWriter printWriter) throws SQLException {
        mainDataSource.setLogWriter(printWriter);
        failoverDataSource.setLogWriter(printWriter);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException(errorMsg);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException(errorMsg);
    }

    public M getMonitor() throws NoMonitorException {
        M t = monitor.get();
        if (t != null) {
            return t;
        } else {
            throw new NoMonitorException(this);
        }
    }

    public String getFailoverDsName() {
        return failoverDsName;
    }

    public String getMainDsName() {
        return mainDsName;
    }

    public String getDsName(boolean failover) {
        return getDsName(failover, false);
    }

    public String getDsName(boolean failover, boolean fixedWidth) {
        return failover ? (fixedWidth ? failoverDsNameFixedWidth : failoverDsName) : (fixedWidth ? mainDsNameFixedWidth : mainDsName);
    }

    public static class NoMonitorException extends Exception {
        public NoMonitorException(Hades hades) {
            super("currently there is no monitor associated with Hades " + hades + "; if this situation lasts longer than couple of minutes then it might be an error - such situation is normal only during initialization");
        }
    }
}
