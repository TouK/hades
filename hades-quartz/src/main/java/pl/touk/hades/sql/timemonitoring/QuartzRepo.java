package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Hades;

public interface QuartzRepo extends Repo {

    State getHadesClusterState(MonitorRunLogPrefix curSyncLogPrefix,
                               Hades hades,
                               long lowerBound,
                               long[] measuringDurationMillis)
            throws InterruptedException;

    void saveHadesClusterState(MonitorRunLogPrefix curRunLogPrefix, Hades hades, State state, long runMethodStartMillis)
            throws InterruptedException;

    String getSchedulerInstanceHumanReadable();
}
