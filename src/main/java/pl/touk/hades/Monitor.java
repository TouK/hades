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

import pl.touk.hades.load.Load;

/**
 * Each instance of this class monitors both data sources of a {@link Hades hades} and can tell if failover should or
 * should not be active for the hades. The associations between hadeses and monitors are "one-to-one".
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public interface Monitor extends ConnectionListener {

    /**
     * Returns <code>true</code> if failover is active and <code>false</code> otherwise. This method controls the
     * process of switching between tha main and the failover data sources contained inside the hades
     * associated with this monitor. For more details see methods {@link Hades#getConnection()} and
     * {@link Hades#getConnection(String, String)} which invoke this method to learn whether failover is
     * active or not.
     *
     * @return <code>true</code> if failover is active or <code>false</code> otherwise
     */
    boolean isFailoverActive();

    /**
     * Returns hades associated with this monitor.
     *
     * @return hades associated with this monitor
     */
    Hades getHades();

    /**
     * Returns current {@link Load load} containing main and failover data base
     * {@link pl.touk.hades.load.LoadLevel load levels} of the associated hades.
     * @return current load of the associated hades
     */
    Load getLoad();
}
