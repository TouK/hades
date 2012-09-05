package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.Monitor;
import pl.touk.hades.load.Load;

public interface SqlTimeBasedMonitor extends Monitor {

    Load getLoad();

    String getLoadLog();

    long getLastQueryTimeMillis(boolean main);

    long getLastQueryTimeNanos(boolean main);
}
