package pl.touk.hades.sql.timemonitoring;

import java.lang.InterruptedException;
import java.lang.String;

/**
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public interface SqlTimeQuartzCalculator extends SqlTimeCalculator {

    State syncValidate(MonitorRunLogPrefix logPrefix, State state) throws InterruptedException;
}
