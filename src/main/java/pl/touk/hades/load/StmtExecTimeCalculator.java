package pl.touk.hades.load;

import pl.touk.hades.HaDataSource;

/**
 * TODO: description
 *
 * @author <a href="mailto:msk@touk.pl">Michał Sokołowski</a>
 */
public interface StmtExecTimeCalculator {
    long calculateStmtExecTimeNanos(HaDataSource haDataSource, String curRunLogPrefix, boolean failover) throws InterruptedException;
    String getStatement();
}
