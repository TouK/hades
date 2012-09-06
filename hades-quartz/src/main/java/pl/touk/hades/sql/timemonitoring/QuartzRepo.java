package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Hades;

public interface QuartzRepo extends Repo {

    State getHadesClusterState(String curSyncLogPrefix, Hades hades, long lowerBound, long[] measuringDurationMillis)
            throws InterruptedException;

    void saveHadesClusterState(String curRunLogPrefix, Hades hades, State state, long runMethodStartMillis)
            throws InterruptedException;
}
