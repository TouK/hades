package pl.touk.hades;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void should() {
        assertEquals(1, Utils.nanosToMillis(1000000));
        assertEquals(-1, Utils.nanosToMillis(-1000000));
        assertEquals(1, Utils.nanosToMillis(1499999));
        assertEquals(-1, Utils.nanosToMillis(-1499999));
        assertEquals(2, Utils.nanosToMillis(1500000));
        assertEquals(-1, Utils.nanosToMillis(-1500000));
        assertEquals(0, Utils.nanosToMillis(499999));
        assertEquals(0, Utils.nanosToMillis(-500000));
        assertEquals(1, Utils.nanosToMillis(500000));
        assertEquals(-1, Utils.nanosToMillis(-500001));
        assertEquals(1, Utils.nanosToMillis(999999));
        assertEquals(-1, Utils.nanosToMillis(-999999));
    }
}
