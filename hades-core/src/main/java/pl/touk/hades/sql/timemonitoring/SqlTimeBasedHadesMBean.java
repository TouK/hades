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

import pl.touk.hades.HadesMBean;

/**
 * An interface implemented by {@link SqlTimeBasedHades} that enables jmx access to the
 * data source. It extends {@link pl.touk.hades.HadesMBean} simply by adding some method
 * specific to load measuring.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public interface SqlTimeBasedHadesMBean extends HadesMBean {

    String getLoadLog();
}
