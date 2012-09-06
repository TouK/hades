/*
 * Copyright 2012 TouK
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
package pl.touk.hades.sql.exception;

import pl.touk.hades.exception.LoadMeasuringException;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public class SqlExecTimeoutSettingException extends LoadMeasuringException {
    public SqlExecTimeoutSettingException(String logPrefix) {
        super(logPrefix);
    }
}
