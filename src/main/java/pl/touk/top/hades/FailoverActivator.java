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

/**
 * The class that controls the switching between the main data source and the failover data source in a
 * {@link HaDataSource}.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public interface FailoverActivator {

    /**
     * Returns <code>true</code> if failover is active and <code>false</code> otherwise. This method is invoked by the
     * HA data source controlled by this activator whenever
     * {@link HaDataSource#getConnection()} or
     * {@link HaDataSource#getConnection(String, String)} is invoked.
     * <p>
     * It is the responsibility of implementations to define when failover and failback should happen.
     *
     * @return <code>true</code> if failover is active or <code>false</code> otherwise
     */
    boolean isFailoverActive();

    /**
     * Gives the control over the given HA data source to this failover activator and initializes this failover activator.
     *
     * @param haDataSource HA data source that should be controlled by this failover activator
     */
    void init(HaDataSource haDataSource);
}
