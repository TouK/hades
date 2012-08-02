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
package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Hades;

import javax.sql.DataSource;

/**
 * A {@link pl.touk.hades.Hades} whose failover activator is of type {@link SqlTimeBasedTriggerImpl}.
 * <p>
 * This class extends <code>HaDataSource</code> simply by implementing jmx operations defined in {@link SqlTimeBasedHadesMBean}.
 * These operations are specific to load measuring done by enclosed <code>SqlTimeBasedTriggerImpl</code>.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class SqlTimeBasedHades extends Hades<SqlTimeBasedTrigger> implements SqlTimeBasedHadesMBean {

    public SqlTimeBasedHades(DataSource mainDataSource, DataSource failoverDataSource, String mainDsName, String failoverDsName) {
        super(mainDataSource, failoverDataSource, mainDsName, failoverDsName);
    }

    public String getFailoverLoad() {
        try {
            return getTrigger().getCurrentState().getLoad().getFailoverDb().name();
        } catch (NoTriggerException e) {
            return e.getMessage();
        }
    }

    public String getMainLoad() {
        try {
            return getTrigger().getCurrentState().getLoad().getMainDb().name();
        } catch (NoTriggerException e) {
            return e.getMessage();
        }
    }

    public String getLoadLog() {
        try {
            return getTrigger().getLoadLog();
        } catch (NoTriggerException e) {
            return e.getMessage();
        }
    }
}
