package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Trigger;
import pl.touk.hades.load.statemachine.MachineState;

public interface SqlTimeBasedTrigger extends Trigger {
    MachineState getCurrentState();

    String getLoadLog();

    SqlTimeCalculator getSqlTimeCalculator();

    String getSchedulerInstanceId();

    String getQuartzCluster();
}
