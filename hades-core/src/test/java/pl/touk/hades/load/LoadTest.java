package pl.touk.hades.load;

import junit.framework.Assert;
import org.junit.Test;
import static pl.touk.hades.load.LoadLevel.low;
import static pl.touk.hades.load.LoadLevel.medium;
import static pl.touk.hades.load.LoadLevel.high;
import static pl.touk.hades.load.LoadLevel.notMeasuredYet;
import static pl.touk.hades.load.LoadLevel.exceptionWhileMeasuring;

public class LoadTest {

    @Test
    public void shouldThrowExceptionIfNotMeasuredYetOnlyForOneLevel() {
        String s = "load with only one load level being notMeasuredYet is illegal";
        ensureExceptionWhileMachineStateCreation(s, notMeasuredYet, low);
        ensureExceptionWhileMachineStateCreation(s, notMeasuredYet, medium);
        ensureExceptionWhileMachineStateCreation(s, notMeasuredYet, high);
        ensureExceptionWhileMachineStateCreation(s, notMeasuredYet, exceptionWhileMeasuring);
        ensureExceptionWhileMachineStateCreation(s, low,                     notMeasuredYet);
        ensureExceptionWhileMachineStateCreation(s, medium,                  notMeasuredYet);
        ensureExceptionWhileMachineStateCreation(s, high,                    notMeasuredYet);
        ensureExceptionWhileMachineStateCreation(s, exceptionWhileMeasuring, notMeasuredYet);
    }

    @Test
    public void shouldThrowExceptionIf() {
        String s = "new Load(notMeasuredYet, true/false) is illegal";
        ensureExceptionWhileMachineStateCreation(s, notMeasuredYet, true);
        ensureExceptionWhileMachineStateCreation(s, notMeasuredYet, false);
    }

    @Test
    public void shouldAllowToCreateLoadWithEqualLoadLevelsInTwoWays() {
        Assert.assertEquals(new Load(low, low),       new Load(low));
        Assert.assertEquals(new Load(medium, medium), new Load(medium));
        Assert.assertEquals(new Load(high, high),     new Load(high));
        Assert.assertEquals(new Load(exceptionWhileMeasuring, exceptionWhileMeasuring),
                                                      new Load(exceptionWhileMeasuring));
    }

    @Test
    public void shouldBeEqual() {
        Assert.assertEquals(new Load(low, medium), new Load(low, medium));
        Assert.assertEquals(new Load(medium, true), new Load(medium, true));
        Assert.assertEquals(new Load(high), new Load(high));
    }

    @Test
    public void shouldNotBeEqual() {
        Assert.assertFalse(new Load(low, medium).equals(new Load(low, high)));
        Assert.assertFalse(new Load(low, medium).equals(new Load(high, medium)));

        Assert.assertFalse(new Load(medium, true).equals(new Load(high, true)));
        Assert.assertFalse(new Load(medium, true).equals(new Load(medium, false)));

        Assert.assertFalse(new Load(high).equals(new Load(medium)));
    }

    @Test
    public void shouldCreateLoad() {
        new Load(low);
        new Load(low, true);
        new Load(low, false);
        new Load(low, low);
        new Load(low, medium);
        new Load(low, high);
        new Load(low, exceptionWhileMeasuring);

        new Load(medium, low);
        new Load(medium);
        new Load(medium, true);
        new Load(medium, false);
        new Load(medium, medium);
        new Load(medium, high);
        new Load(medium, exceptionWhileMeasuring);

        new Load(high, low);
        new Load(high, medium);
        new Load(high);
        new Load(high, true);
        new Load(high, false);
        new Load(high, high);
        new Load(high, exceptionWhileMeasuring);

        new Load(exceptionWhileMeasuring, low);
        new Load(exceptionWhileMeasuring, medium);
        new Load(exceptionWhileMeasuring, high);
        new Load(exceptionWhileMeasuring);
        new Load(exceptionWhileMeasuring, true);
        new Load(exceptionWhileMeasuring, false);
        new Load(exceptionWhileMeasuring, exceptionWhileMeasuring);
        
        new Load(notMeasuredYet, notMeasuredYet);
        new Load(notMeasuredYet);
    }

    private void ensureExceptionWhileMachineStateCreation(String infix, LoadLevel main, LoadLevel failover) {
        ensureExceptionWhileMachineStateCreation(infix, main, failover, null);
    }

    private void ensureExceptionWhileMachineStateCreation(String infix, LoadLevel l, boolean mainIsHigher) {
        ensureExceptionWhileMachineStateCreation(infix, l, null, mainIsHigher);
    }

    private void ensureExceptionWhileMachineStateCreation(String infix, LoadLevel main, LoadLevel failover, Boolean higher) {
        try {
            if (higher == null) {
                new Load(main, failover);
            } else {
                new Load(main, higher);
            }
            Assert.fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("'" + e.getMessage() + "' does not contain '" + infix + "'", e.getMessage().contains(infix));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullMainAndFailoverDbLoadLevel() {
        new Load(null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullMainDbLoadLevel() {
        new Load(null, low);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullFailoverDbLoadLevel() {
        new Load(low, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullLoadLevel() {
        new Load(null);
    }
}
