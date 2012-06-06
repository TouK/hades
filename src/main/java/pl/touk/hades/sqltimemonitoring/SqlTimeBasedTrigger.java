package pl.touk.hades.sqltimemonitoring;

import org.quartz.Scheduler;
import pl.touk.hades.Trigger;
import pl.touk.hades.load.statemachine.MachineState;

public interface SqlTimeBasedTrigger extends Trigger {
    void setSqlTimeTriggeringFailoverMillis(int sqlTimeTriggeringFailoverMillis);

    void setSqlTimeTriggeringFailbackMillis(int sqlTimeTriggeringFailbackMillis);

    void setSqlTimesIncludedInAverage(int sqlTimesIncludedInAverage);

    void setExceptionsIgnoredAfterRecovery(boolean exceptionsIgnoredAfterRecovery);

    void setRecoveryErasesHistoryIfExceptionsIgnoredAfterRecovery(boolean recoveryErasesHistoryIfExceptionsIgnoredAfterRecovery);

    void setSqlTimeCalculator(SqlTimeCalculator sqlTimeCalculator);

    void setCron(String cron);

    void setScheduler(Scheduler scheduler);

    MachineState getCurrentState();

    String getLoadLog();
}
