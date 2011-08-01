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

import pl.touk.hades.HaDataSource;

/**
 * A {@link pl.touk.hades.HaDataSource} whose failover activator is of type {@link LoadFailoverActivator}.
 * <p>
 * This class extends <code>HaDataSource</code> simply by implementing jmx operations defined in {@link LoadHaDataSourceMBean}.
 * These operations are specific to load measuring done by enclosed <code>LoadFailoverActivator</code>.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class LoadHaDataSource extends HaDataSource<LoadFailoverActivator> implements LoadHaDataSourceMBean {

    public String getFailoverLoad() {
        return getFailoverActivator().getCurrentState().getLoad().getFailoverDb().name();
    }

    public String getMainLoad() {
        return getFailoverActivator().getCurrentState().getLoad().getMainDb().name();
    }

    public String getLoadLog() {
        return getFailoverActivator().getLoadLog();
    }
}
