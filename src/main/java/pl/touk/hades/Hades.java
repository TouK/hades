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
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.exception.ConnectionGettingException;
import pl.touk.hades.Trigger;

/**
 * A data source which offers high-availability (HA) by enclosing two real data sources - the main one and the failover
 * one. Which enclosed data source is used is not determined by the HA data source itself. It is the responibility of
 * {@link Trigger} associated with this HA data source.
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
public class Hades<T extends Trigger> implements DataSource, HadesMBean {

    static private final Logger logger = LoggerFactory.getLogger(Hades.class);

    static private final String errorMsg = "this class wraps two data sources so this method is irrelevant as it does not give the possbility to specify which data source should it operate on";

    static public final long nanosInMillisecond = 1000000L;

    public static final String defaultMainDataSourceName = "main data source";
    public static final String defaultFailoverDataSourceName = "failover data source";

    private String name;

    private DataSource mainDataSource;
    private DataSource failoverDataSource;
    private String mainDataSourceName = defaultMainDataSourceName;
    private String failoverDataSourceName = defaultFailoverDataSourceName;

    private T trigger;

    private final String pinGuard = "";
    private Boolean failoverDataSourcePinned = null;

    private final DecimalFormat decimalFormat = new DecimalFormat("#0.000 ms");

    /**
     * Associates the failover activator set by {@link #setTrigger(Trigger)} with this HA data
     * source. This method simply invokes
     * {@link #getTrigger ()}.{@link Trigger#init(Hades) init(this)}.
     */
    public void init() {
        getTrigger().init(this);
    }

    /**
     * Invokes {@link javax.sql.DataSource#getConnection() getConnection()} on the main data source or on the failover
     * data source if failover is inactive or active respectively.
     * To check whether failover is active {@link Trigger#isFailoverActive() isFailoverActive()} on the
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
        return getConnection(failoverEffectivelyActive(), false, null, null, "");
    }

    /**
     * Invokes {@link javax.sql.DataSource#getConnection(String, String) getConnection(username, password)} on the main
     * data source or on the failover data source if failover is inactive or active respectively.
     * To check whether failover is active {@link Trigger#isFailoverActive() isFailoverActive()} on the
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
        return getConnection(failoverEffectivelyActive(), true, username, password, "");
    }

    /**
     * Delegates to {@link #getConnection(boolean, boolean, String, String, String)} simply surrounding possible
     * thrown exceptions by {@link ConnectionGettingException}.
     *
     * @param failover whether to return a connection from the failover data source or from the main data source
     * @param logPrefix text used as a log prefix
     * @return connection from the specified data source
     * @throws ConnectionGettingException if the specified data source throws the runtime exception
     */
    public Connection getConnection(boolean failover, String logPrefix) throws ConnectionGettingException {
        try {
            return getConnection(failover, false, null, null, logPrefix);
        } catch (Exception e) {
            throw new ConnectionGettingException(logPrefix, e);
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
     * @param failover whether to return a connection from the failover data source or from the main data source
     * @param withAuth whether to delegate to {@link DataSource#getConnection(String, String)} or to {@link DataSource#getConnection()}
     * @param username username for which the connection should be returned (ignored if <code>withAuth</code> is <code>false</code>)
     * @param password password for the given <code>username</code> (ignored if <code>withAuth</code> is <code>false</code>)
     * @param logPrefix text used as a log prefix
     * @return connection from the specified data source
     * @throws SQLException if the specified data source throws the exception
     * @throws RuntimeException if the specified data source throws the runtime exception
     */
    public Connection getConnection(boolean failover, boolean withAuth, String username, String password, String logPrefix) throws SQLException, RuntimeException {
        DataSource ds = failover ? failoverDataSource : mainDataSource;
        Connection connection;
        long start = System.nanoTime();
        try {
            connection = withAuth ? ds.getConnection(username, password) : ds.getConnection();
        } catch (SQLException e) {
            throw handleException(e, start, failover,  withAuth, username, logPrefix);
        } catch (RuntimeException e) {
            throw handleException(e, start, failover,  withAuth, username, logPrefix);
        }
        long timeElapsedNanos = System.nanoTime() - start;
        if (logger.isDebugEnabled()) {
            logger.debug(logPrefix + "successfully got a connection" + (withAuth ? " for username " + username : "") + " to " + (failover ? failoverDataSourceName + " (failover": mainDataSourceName + " (main") + " ds) in " + nanosToMillis(timeElapsedNanos));
        }
        return connection;
    }

    private <T extends Throwable> T handleException(T e, long start, boolean failover, boolean withAuth, String username, String logPrefix) throws T {
        long timeElapsedNanos = System.nanoTime() - start;
        logger.error(logPrefix + "exception while getting a connection" + (withAuth ? " for username " + username : "") + " to " + (failover ? failoverDataSourceName + " (failover" : mainDataSourceName + " (main") + " ds) caught in " + nanosToMillis(timeElapsedNanos), e);
        return e;
    }

    @Override
    public String toString() {
        return mainDataSourceName + "/" + failoverDataSourceName;
    }

    public String desc(boolean failover) {
        return (failover ? getFailoverDataSourceName() + " (failover" : getMainDataSourceName() + " (main") + " ds)";
    }

    private String nanosToMillis(long l) {
        return decimalFormat.format(((double) l) / nanosInMillisecond);
    }

    private boolean failoverEffectivelyActive() {
        synchronized (pinGuard) {
            if (failoverDataSourcePinned != null) {
                return failoverDataSourcePinned;
            }
        }
        return trigger.isFailoverActive();
    }

    /**
     * Pins the main data source. From now on methods {@link #getConnection()} and {@link #getConnection(String, String)}
     * will get connections from the main data source.
     */
    public void pinMainDataSource() {
        synchronized (pinGuard) {
            failoverDataSourcePinned = false;
        }
    }

    /**
     * Pins the failover data source. From now on methods {@link #getConnection()} and {@link #getConnection(String, String)}
     * will get connections from the failover data source.
     */
    public void pinFailoverDataSource() {
        synchronized (pinGuard) {
            failoverDataSourcePinned = true;
        }
    }

    /**
     * Removes pin enabled by {@link #pinMainDataSource()} or {@link #pinFailoverDataSource()}. From now on methods
     * {@link #getConnection()} and {@link #getConnection(String, String)} will ask the failover activator whether
     * failover is active.
     */
    public void removePin() {
        synchronized (pinGuard) {
            failoverDataSourcePinned = null;
        }
    }

    /**
     * Returnes <code>true</code> if pin is enabled and <code>false</code> otherwise. The pin can be enabled by
     * {@link #pinMainDataSource()} or {@link #pinFailoverDataSource()}. The pin can be disabled by
     * {@link #removePin()}.
     *
     * @return <code>true</code> if pin is enabled and <code>false</code> otherwise
     */
    public boolean getPinEnabled() {
        synchronized (pinGuard) {
            return failoverDataSourcePinned != null;
        }
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
        synchronized (pinGuard) {
            return failoverDataSourcePinned;
        }
    }

    /**
     * Returns <code>true</code> if the failover data source is used and <code>false</code> if the main data source is used.
     * The failover data source is used (i.e. {@link #getConnection()} and {@link #getConnection(String, String)}
     * returns connections from it) if it is pinned (see {@link #pinFailoverDataSource()}). Similarly, the main data
     * source is used if it is pinned (see {@link #pinMainDataSource()}). If no pin is enabled and
     * {@link #getTrigger()}.{@link Trigger#isFailoverActive() isFailoverActive()} returns
     * <code>false</code> the main data source is used.
     * If no pin is enabled and
     * {@link #getTrigger ()}.{@link Trigger#isFailoverActive() isFailoverActive()} returns
     * <code>true</code> the failover data source is used.
     *
     * @return <code>true</code> if the failover data source is used, <code>false</code> otherwise
     */
    public boolean isFailoverActive() {
        return failoverEffectivelyActive();
    }

    /**
     * Returns the name of the currently used data source. The currently used data source is determined as in
     * {@link #isFailoverActive()}. The main data source name can be set in {@link #setMainDataSourceName(String)}.
     * The failover data source name can be set in {@link #setFailoverDataSourceName(String)}. The default names
     * are {@link #defaultMainDataSourceName} and {@link #defaultFailoverDataSourceName}.
     *
     * @return the name of the currently used data source
     */
    public String getActiveDataSourceName() {
        return failoverEffectivelyActive() ? getFailoverDataSourceName() : getMainDataSourceName();
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

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
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

    /**
     * Throws <code>UnsupportedOperationException</code>.
     *
     * @throws UnsupportedOperationException always
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException(errorMsg);
    }

    /**
     * Throws <code>UnsupportedOperationException</code>.
     *
     * @throws UnsupportedOperationException always
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException(errorMsg);
    }

    /**
     * Gets the failover activator set by {@link #setTrigger(Trigger)}.
     *
     * @return failover activator controlling this HA data source
     */
    protected T getTrigger() {
        return trigger;
    }

    /**
     * Sets the failover data source.
     *
     * @param failoverDataSource failover data source
     */
    public void setFailoverDataSource(DataSource failoverDataSource) {
        this.failoverDataSource = failoverDataSource;
    }

    /**
     * Sets the main data source.
     *
     * @param mainDataSource main data source
     */
    public void setMainDataSource(DataSource mainDataSource) {
        this.mainDataSource = mainDataSource;
    }

    /**
     * Sets the name of the failover data source.
     *
     * @param failoverDataSourceName name of the failover data source
     */
    public void setFailoverDataSourceName(String failoverDataSourceName) {
        this.failoverDataSourceName = failoverDataSourceName;
    }

    /**
     * Gets the name of the failover data source. The name is used in logs.
     *
     * @return name of the failover data source
     */
    public String getFailoverDataSourceName() {
        return failoverDataSourceName;
    }

    /**
     * Sets the name of the main data source.
     *
     * @param mainDataSourceName name of the main data source
     */
    public void setMainDataSourceName(String mainDataSourceName) {
        this.mainDataSourceName = mainDataSourceName;
    }

    /**
     * Gets the name of the main data source. The name is used in logs.
     *
     * @return name of the main data source
     */
    public String getMainDataSourceName() {
        return mainDataSourceName;
    }

    /**
     * Sets the failover activator that will control this HA data source.
     *
     * @param trigger failover activator that should control this HA data source
     */
    public void setTrigger(T trigger) {
        this.trigger = trigger;
    }
}
