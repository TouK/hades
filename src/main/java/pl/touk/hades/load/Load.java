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
package pl.touk.hades.load;

import java.util.Date;
import java.io.Serializable;

/**
 * A holder for the main data source {@link LoadLevel load level} and the failover data source load level. If the two
 * load levels are equal (for example both are {@link LoadLevel#low low} than this class can also hold information
 * whether the main database load level is higher.
 * <p>
 * Instances of this class are immutable.
 *
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class Load implements Serializable {

    private static final long serialVersionUID = -5166147802715344969L;

    private final LoadLevel mainDb;
    private final LoadLevel failoverDb;

    // If <mainDb, failoverDb> = <low, low> then it is unimportant whether mainDb is higher than failoverDb or not - in
    // neither case is the failover active. But if for example <mainDb, failoverDb> = <high, high> then we need
    // more information to decide whether failover is active or not: we need to know whether mainDb load level is higher
    // than the failoverDb load level and this information can be provided by the following field:
    private final Boolean mainDbLoadHigher;

    private Load(LoadLevel mainDb, LoadLevel failoverDb, Boolean mainDbLoadHigher) {
        if (mainDb == null) {
            throw new IllegalArgumentException("null mainDb");
        }
        if (failoverDb == null) {
            throw new IllegalArgumentException("null failoverDb");
        }
        this.mainDb = mainDb;
        this.failoverDb = failoverDb;
        this.mainDbLoadHigher = mainDbLoadHigher;
    }

    /**
     * Creates <code>Load</code> with the given load levels.
     *
     * @param mainDb main database load level
     * @param failoverDb failover database load level
     */
    public Load(LoadLevel mainDb, LoadLevel failoverDb) {
        this(mainDb, failoverDb, null);
    }

    public Load(LoadLevel loadLevel, boolean mainDbLoadHigher) {
        this(loadLevel, loadLevel, mainDbLoadHigher);
    }

    public Load(LoadLevel loadLevel) {
        this(loadLevel, loadLevel, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Load load = (Load) o;

        if (failoverDb != load.failoverDb) return false;
        if (mainDb != load.mainDb) return false;
        if (mainDbLoadHigher != null ? !mainDbLoadHigher.equals(load.mainDbLoadHigher) : load.mainDbLoadHigher != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = mainDb != null ? mainDb.hashCode() : 0;
        result = 31 * result + (failoverDb != null ? failoverDb.hashCode() : 0);
        result = 31 * result + (mainDbLoadHigher != null ? mainDbLoadHigher.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("{main=").append(mainDb).append(",failover=").append(failoverDb);
        if (mainDbLoadHigher != null) {
            sb.append(",mainHigher=").append(mainDbLoadHigher);
        }
        sb.append("}");
        return sb.toString();
    }

    public LoadLevel getFailoverDb() {
        return failoverDb;
    }

    public LoadLevel getMainDb() {
        return mainDb;
    }

    public Boolean isMainDbLoadHigher() {
        return mainDbLoadHigher;
    }

    public boolean canBeGeneralized() {
        return mainDbLoadHigher != null;
    }

    public Load generalize() {
        return new Load(mainDb, failoverDb);
    }
}
