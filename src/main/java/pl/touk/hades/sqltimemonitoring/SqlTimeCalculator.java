package pl.touk.hades.sqltimemonitoring;

import pl.touk.hades.Hades;
import pl.touk.hades.exception.*;

/**
 * TODO: description
 *
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public interface SqlTimeCalculator {
    long calculateSqlTimeNanos(Hades haDataSource, String curRunLogPrefix, boolean failover) throws InterruptedException;
    String getSql();
}
