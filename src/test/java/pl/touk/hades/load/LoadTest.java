package pl.touk.hades.load;

import org.junit.Test;

public class LoadTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullMainAndFailoverDbLoadLevel() {
        new Load(null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullMainDbLoadLevel() {
        new Load(null, LoadLevel.low);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullFailoverDbLoadLevel() {
        new Load(LoadLevel.low, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullLoadLevel() {
        new Load(null);
    }
}
