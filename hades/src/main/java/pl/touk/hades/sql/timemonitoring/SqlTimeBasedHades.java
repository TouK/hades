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
 * {@link pl.touk.hades.Hades} whose monitor is of type {@link pl.touk.hades.sql.timemonitoring.SqlTimeBasedMonitorImpl}.
 * <p>
 * This class extends Hades simply by implementing jmx operations defined in {@link SqlTimeBasedHadesMBean}.
 * These operations are specific to sql time measuring done by the associated {@link SqlTimeBasedMonitor}.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class SqlTimeBasedHades extends Hades<SqlTimeBasedMonitor> implements SqlTimeBasedHadesMBean {

    public SqlTimeBasedHades(DataSource mainDataSource, DataSource failoverDataSource, String mainDsName, String failoverDsName) {
        super(mainDataSource, failoverDataSource, mainDsName, failoverDsName);
    }

    public String getLoadLog() {
        try {
            return getMonitor().getLoadLog();
        } catch (NoMonitorException e) {
            return e.getMessage();
        }
    }
}
