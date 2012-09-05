package pl.touk.hades.sql.timemonitoring;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StateTest {

    private final static long failoverThreshold = 10;
    private final static long failbackThreshold = 5;

    private final static long low1 = 1;
    private final static long low2 = 2;
    private final static long medium1 = 6;
    private final static long medium2 = 7;
    private final static long high1 = 11;
    private final static long high2 = 12;

    private final static int normalPeriod = 1;
    private final static int somePeriodWhenUnused = 44;
    private final static int periodWhenUnused = 4;

    private final static boolean mainDb = false;
    private final static boolean failoverDb = true;

    @Test
    public void shouldBeInInitialState() {
        // given:
        State s;

        // when:
        s = createInitialLocalState(somePeriodWhenUnused, 1);

        // then:
        assertTrue(s.sqlTimeIsMeasuredInThisCycle(mainDb));
        assertTrue(s.sqlTimeIsMeasuredInThisCycle(failoverDb));
        assertEquals(false, s.getMachineState().isFailoverActive());
        assertEquals(normalPeriod, s.getPeriod(mainDb));
        assertEquals(normalPeriod, s.getPeriod(failoverDb));
        assertEquals(0, s.getCycleModuloPeriod(mainDb));
        assertEquals(0, s.getCycleModuloPeriod(failoverDb));
    }

    @Test
    public void shouldDecreaseLoadOfFailoverDatabaseIfUnusedAfterFirstMeasurement() {
        // given:
        int periodWhenUnused = 2;
        State s = createInitialLocalState(periodWhenUnused, 1);

        // when:
        s.updateLocalStateWithNewExecTimes("", low1, low1, null);

        // then:
        assertTrue(s.sqlTimeIsMeasuredInThisCycle(mainDb));
        assertFalse(s.sqlTimeIsMeasuredInThisCycle(failoverDb));
        assertEquals(false, s.getMachineState().isFailoverActive());
        assertEquals(normalPeriod, s.getPeriod(mainDb));
        assertEquals(periodWhenUnused, s.getPeriod(failoverDb));
        assertEquals(0, s.getCycleModuloPeriod(mainDb));
        assertEquals(1, s.getCycleModuloPeriod(failoverDb));
    }

    @Test
    public void shouldKeepDecreasedLoadOfFailoverDatabaseIfStillUnused() {
        // given:
        State s = createInitialLocalState(4, 1);

        // when:
        s.updateLocalStateWithNewExecTimes("", low1, low1, null);

        // then:
        assertEquals(false, s.sqlTimeIsMeasuredInThisCycle(true));

        // when:
        s.updateLocalStateWithNewExecTimes("", low1, State.notMeasuredInThisCycle, null);

        // then:
        assertDecreasedLoadOfFailoverDatabase(s, 2);
        assertEquals(false, s.sqlTimeIsMeasuredInThisCycle(true));

        // when:
        s.updateLocalStateWithNewExecTimes("", low1, State.notMeasuredInThisCycle, null);

        // then:
        assertDecreasedLoadOfFailoverDatabase(s, 3);
    }

    @Test
    public void shouldDecreaseLoadOfFailoverDatabaseIfProblemsAfterNotUnused() {
        // given:
        int periodWhenUnused = 2;
        int backOffMultiplier = 3;
        State s = createInitialLocalState(periodWhenUnused, backOffMultiplier);
        s.updateLocalStateWithNewExecTimes("", low1, low1, null);
        s.updateLocalStateWithNewExecTimes("", low1, State.notMeasuredInThisCycle, null);

        // when:
        s.updateLocalStateWithNewExecTimes("", low1, ExceptionEnum.connException.value(), null);

        // then:
        assertEquals(periodWhenUnused * backOffMultiplier, s.getPeriod(true));
    }

    private void assertDecreasedLoadOfFailoverDatabase(State s, int cyclePerPeriodForFailoverDb) {
        assertTrue(s.sqlTimeIsMeasuredInThisCycle(mainDb));
        assertFalse(s.sqlTimeIsMeasuredInThisCycle(failoverDb));
        assertEquals(false, s.getMachineState().isFailoverActive());
        assertEquals(normalPeriod, s.getPeriod(mainDb));
        assertEquals(periodWhenUnused, s.getPeriod(failoverDb));
        assertEquals(0, s.getCycleModuloPeriod(mainDb));
        assertEquals(cyclePerPeriodForFailoverDb, s.getCycleModuloPeriod(failoverDb));
    }

    @Test
    public void shouldDecreaseLoadOfMainDatabaseIfUnusedAfterFirstMeasurement() {
        // given:
        int periodWhenUnused = 2;
        int backOffMultiplier = 3;
        State s = createInitialLocalState(periodWhenUnused, backOffMultiplier);

        // when:
        s.updateLocalStateWithNewExecTimes("", high1, low1, null);

        // then:
        assertFalse(s.sqlTimeIsMeasuredInThisCycle(mainDb));
        assertTrue(s.sqlTimeIsMeasuredInThisCycle(failoverDb));
        assertEquals(true, s.getMachineState().isFailoverActive());
        assertEquals(backOffMultiplier, s.getPeriod(mainDb));
        assertEquals(normalPeriod, s.getPeriod(failoverDb));
        assertEquals(1, s.getCycleModuloPeriod(mainDb));
        assertEquals(0, s.getCycleModuloPeriod(failoverDb));
    }

    private State createInitialLocalState(int periodWhenUnused, int backOffMultiplier) {
        return new State(new SqlTimeBasedLoadFactory(failoverThreshold, failbackThreshold), null, 1, false, false, periodWhenUnused, backOffMultiplier, 100, "MAIN_DB", "FAILOVER_DB");
    }
}
