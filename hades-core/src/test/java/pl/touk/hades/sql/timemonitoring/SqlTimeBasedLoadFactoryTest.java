package pl.touk.hades.sql.timemonitoring;

import junit.framework.Assert;
import org.junit.Test;
import pl.touk.hades.exception.LoadMeasuringException;
import pl.touk.hades.exception.UnexpectedException;
import pl.touk.hades.load.Load;
import pl.touk.hades.load.LoadLevel;
import pl.touk.hades.sql.exception.*;

import static pl.touk.hades.load.LoadLevel.*;

public class SqlTimeBasedLoadFactoryTest {

    @Test
    public void shouldCreateFactory() {
        new SqlTimeBasedLoadFactory(1, 1);
        new SqlTimeBasedLoadFactory(2, 1);
    }

    @Test
    public void shouldNotAllowNonPositiveThreshold() {
        String s = "failoverThresholdNanos <= 0";
        ensureExceptionWhileCreatingFactory(s, -1, 100);
        ensureExceptionWhileCreatingFactory(s, 0, 100);
        s = "failbackThresholdNanos <= 0";
        ensureExceptionWhileCreatingFactory(s, 100, -1);
        ensureExceptionWhileCreatingFactory(s, 100, 0);
    }

    @Test
    public void shouldNotAllowErroneousThreshold() {
        String s1 = "failoverThresholdNanos >= minimum erroneous value";
        String s2 = "failbackThresholdNanos >= minimum erroneous value";
        for(long l = Long.MAX_VALUE; l >= ExceptionEnum.minErroneousValue(); l--) {
            ensureExceptionWhileCreatingFactory(s1, l, 100);
            ensureExceptionWhileCreatingFactory(s1, l, l);
            ensureExceptionWhileCreatingFactory(s2, 100, l);
        }
    }

    @Test
    public void testName() throws Exception {
        assertLoad(low, null, false, 4, 2, 1, 2);
        assertLoad(low, null, true, 4, 2, 2, 1);
        assertLoad(low, null, false, 4, 2, 1, 1);
        assertLoad(low, null, false, 4, 2, 2, 2);

        assertLoad(low, medium, null, 4, 2, 1, 3);
        assertLoad(low, medium, null, 4, 2, 1, 4);
        assertLoad(low, medium, null, 4, 2, 2, 3);
        assertLoad(low, medium, null, 4, 2, 2, 4);

        assertLoad(low, high, null, 4, 2, 1, 5);
        assertLoad(low, high, null, 4, 2, 1, 6);
        assertLoad(low, high, null, 4, 2, 2, 6);
        assertLoad(low, high, null, 4, 2, 2, 7);

        assertLoad(low, exceptionWhileMeasuring, null, 4, 2, 1, ExceptionEnum.minErroneousValue());
        assertLoad(low, exceptionWhileMeasuring, null, 4, 2, 1, ExceptionEnum.minErroneousValue() + 1);

        assertLoad(medium, low, null, 4, 2, 3, 2);
        assertLoad(medium, low, null, 4, 2, 4, 1);
        assertLoad(medium, low, null, 4, 2, 3, 1);
        assertLoad(medium, low, null, 4, 2, 4, 2);

        assertLoad(medium, null, false, 4, 2, 3, 4);
        assertLoad(medium, null, true, 4, 2, 4, 3);
        assertLoad(medium, null, false, 4, 2, 3, 3);
        assertLoad(medium, null, false, 4, 2, 4, 4);

        assertLoad(medium, high, null, 4, 2, 3, 5);
        assertLoad(medium, high, null, 4, 2, 3, 6);
        assertLoad(medium, high, null, 4, 2, 4, 6);
        assertLoad(medium, high, null, 4, 2, 4, 7);

        assertLoad(medium, exceptionWhileMeasuring, null, 4, 2, 3, ExceptionEnum.minErroneousValue());
        assertLoad(medium, exceptionWhileMeasuring, null, 4, 2, 3, ExceptionEnum.minErroneousValue() + 1);

        assertLoad(high, low, null, 4, 2, 6, 2);
        assertLoad(high, low, null, 4, 2, 5, 1);
        assertLoad(high, low, null, 4, 2, 5, 1);
        assertLoad(high, low, null, 4, 2, 6, 2);

        assertLoad(high, medium, null, 4, 2, 5, 4);
        assertLoad(high, medium, null, 4, 2, 6, 3);
        assertLoad(high, medium, null, 4, 2, 5, 3);
        assertLoad(high, medium, null, 4, 2, 6, 4);

        assertLoad(high, null, false, 4, 2, 5, 5);
        assertLoad(high, null, true, 4, 2, 6, 5);
        assertLoad(high, null, false, 4, 2, 6, 6);
        assertLoad(high, null, false, 4, 2, 6, 7);

        assertLoad(high, exceptionWhileMeasuring, null, 4, 2, 5, ExceptionEnum.minErroneousValue());
        assertLoad(high, exceptionWhileMeasuring, null, 4, 2, 6, ExceptionEnum.minErroneousValue() + 1);

        assertLoad(exceptionWhileMeasuring, low, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 2);
        assertLoad(exceptionWhileMeasuring, low, null, 4, 2, ExceptionEnum.minErroneousValue(), 1);
        assertLoad(exceptionWhileMeasuring, low, null, 4, 2, ExceptionEnum.minErroneousValue(), 1);
        assertLoad(exceptionWhileMeasuring, low, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 2);

        assertLoad(exceptionWhileMeasuring, medium, null, 4, 2, ExceptionEnum.minErroneousValue(), 4);
        assertLoad(exceptionWhileMeasuring, medium, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 3);
        assertLoad(exceptionWhileMeasuring, medium, null, 4, 2, ExceptionEnum.minErroneousValue(), 3);
        assertLoad(exceptionWhileMeasuring, medium, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 4);

        assertLoad(exceptionWhileMeasuring, high, null, 4, 2, ExceptionEnum.minErroneousValue(), 5);
        assertLoad(exceptionWhileMeasuring, high, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 5);
        assertLoad(exceptionWhileMeasuring, high, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 6);
        assertLoad(exceptionWhileMeasuring, high, null, 4, 2, ExceptionEnum.minErroneousValue() + 1, 7);

        assertLoad(exceptionWhileMeasuring, exceptionWhileMeasuring, false, 4, 2, ExceptionEnum.minErroneousValue(), ExceptionEnum.minErroneousValue());
        assertLoad(exceptionWhileMeasuring, exceptionWhileMeasuring, false, 4, 2, ExceptionEnum.minErroneousValue() + 1, ExceptionEnum.minErroneousValue() + 1);
        assertLoad(exceptionWhileMeasuring, exceptionWhileMeasuring, true, 4, 2, ExceptionEnum.minErroneousValue() + 2, ExceptionEnum.minErroneousValue() + 1);
        assertLoad(exceptionWhileMeasuring, exceptionWhileMeasuring, false, 4, 2, ExceptionEnum.minErroneousValue() + 2, ExceptionEnum.minErroneousValue() + 3);
    }

    @Test
    public void shouldReturnExceptionWhileMeasuringForLoadMeasuringException() throws Exception {
        shouldReturnExceptionWhileMeasuringForLoadMeasuringException(true);
        shouldReturnExceptionWhileMeasuringForLoadMeasuringException(false);
    }

    private void shouldReturnExceptionWhileMeasuringForLoadMeasuringException(boolean failover) throws Exception {
        assertException(failover, new LoadMeasuringException(""));
        assertException(failover, new ConnException(""));
        assertException(failover, new ConnTimeout(""));
        assertException(failover, new PrepareStmtException(""));
        assertException(failover, new SqlExecException(""));
        assertException(failover, new SqlExecTimeout(""));
        assertException(failover, new SqlExecTimeoutSettingException(""));
        assertException(failover, new UnexpectedException(""));
    }

    private void assertException(boolean failover, LoadMeasuringException e) {
        if (failover) {
            Assert.assertEquals(exceptionWhileMeasuring, new SqlTimeBasedLoadFactory(2, 1).getLoad(1, ExceptionEnum.valueForException(e)).getFailoverDb());
        } else {
            Assert.assertEquals(exceptionWhileMeasuring, new SqlTimeBasedLoadFactory(2, 1).getLoad(ExceptionEnum.valueForException(e), 1).getMainDb());
        }
    }

    private void assertLoad(LoadLevel l1, LoadLevel l2, Boolean mainHigher, long failoverThreshold, long failbackThreshold, long main, long failover) {
        if (mainHigher != null) {
            Assert.assertEquals(new Load(l1, mainHigher), new SqlTimeBasedLoadFactory(failoverThreshold, failbackThreshold).getLoad(main, failover));
        } else {
            Assert.assertEquals(new Load(l1, l2), new SqlTimeBasedLoadFactory(failoverThreshold, failbackThreshold).getLoad(main, failover));
        }
    }

    @Test
    public void shouldNotAllowFailoverThresholdLowerThanFailbackThreshold() throws Exception {
        ensureExceptionWhileCreatingFactory("failoverThresholdNanos < failbackThresholdNanos", 1, 2);
    }

    private void ensureExceptionWhileCreatingFactory(String infix, long failoverThreshold, long failbackThreshold) {
        try {
            new SqlTimeBasedLoadFactory(failoverThreshold, failbackThreshold);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("'" + e.getMessage() + "' does not contain '" + infix + "'", e.getMessage().contains(infix));
        }
    }
}
