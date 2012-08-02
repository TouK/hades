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
 * The class that controls the switching between the main and the failover data sources in a
 * {@link Hades}. In other words this class can trigger failover or failback.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public interface Trigger {

    /**
     * Returns <code>true</code> if failover is active and <code>false</code> otherwise. This method controls the
     * process of switching between tha main and the failover data sources contained inside the HA data source
     * associated with this failover activator. For more details see methods {@link Hades#getConnection()} and
     * {@link Hades#getConnection(String, String)} which invoke this method to learn whether failover is
     * active or not.
     *
     * @return <code>true</code> if failover is active or <code>false</code> otherwise
     */
    boolean isFailoverActive();

    Hades getHades();

    void connectionRequested(boolean success, boolean failover, long timeNanos);

    long getLastFailoverQueryTimeMillis(boolean main);
}
