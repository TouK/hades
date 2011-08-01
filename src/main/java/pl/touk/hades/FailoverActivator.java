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

/**
 * The class that controls the switching between the main and the failover data sources in a {@link HaDataSource}.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public interface FailoverActivator {

    /**
     * Returns <code>true</code> if failover is active and <code>false</code> otherwise. This method controls the
     * process of switching between tha main and the failover data sources contained inside the HA data source
     * associated with this failover activator. For more details see methods {@link HaDataSource#getConnection()} and
     * {@link HaDataSource#getConnection(String, String)} which invoke this method to learn whether failover is
     * active or not.
     *
     * @return <code>true</code> if failover is active or <code>false</code> otherwise
     */
    boolean isFailoverActive();

    /**
     * Associates the given HA data source with this failover activator and initializes this failover activator.
     * This method should not be invoked directly.
     * It is invoked by haDataSource.{@link pl.touk.hades.HaDataSource#init() init()} as part of the
     * association process that should be done as follows:
     * <pre>
     * haDataSource.{@link pl.touk.hades.HaDataSource#setFailoverActivator(FailoverActivator) setFailoverActivator(thisFailoverActivator)};
     * haDataSource.{@link pl.touk.hades.HaDataSource#init() init()};
     * </pre>
     * The purpose of this method is to give an implementation a chance to initialize itself whenever it is
     * associated with a HA data source.
     *
     * @param haDataSource HA data source that should be associated with this failover activator
     */
    void init(HaDataSource haDataSource);
}
