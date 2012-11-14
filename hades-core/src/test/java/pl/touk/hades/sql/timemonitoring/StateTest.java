package pl.touk.hades.sql.timemonitoring;

import org.junit.Test;
import pl.touk.hades.load.LoadLevel;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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

    private final static String initialHost = "initial host";
    private final static String host        = "host";

    private final static MonitorRunLogPrefix emptyLogPrefix = new MonitorRunLogPrefix();

    @Test
    public void shouldUpdate() throws InterruptedException {
        // given:
        int backOffMultiplier = 3;
        State state = createInitialLocalState(periodWhenUnused, backOffMultiplier);
        State beforeUpdate = state.clone();
        State returnedState;
        long before;
        long after;

        // when:
        before = System.currentTimeMillis();
        Thread.sleep(1);
        returnedState = state.updateLocalStateWithNewExecTimes(emptyLogPrefix, high1, medium1);
        Thread.sleep(1);
        after = System.currentTimeMillis();

        // then:
        assertEquals(returnedState, beforeUpdate);
        assertTrue(returnedState != beforeUpdate);
        assertFalse(beforeUpdate.equals(state));
        assertTrue(state.getMachineState().isFailoverActive());

        assertEquals(high1, state.getAvg().getValue());
        assertEquals(LoadLevel.high, state.getLoadAfterLastMeasurement().getLoadLevel(false));
        assertEquals(1, state.getCycleModuloPeriod(false));
        assertEquals(backOffMultiplier, state.getPeriod(false));

        assertEquals(medium1, state.getAvgFailover().getValue());
        assertEquals(LoadLevel.medium, state.getLoadAfterLastMeasurement().getLoadLevel(true));
        assertEquals(0, state.getCycleModuloPeriod(true));
        assertEquals(normalPeriod, state.getPeriod(true));

        assertTrue(before < state.getModifyTimeMillis());
        assertTrue(after > state.getModifyTimeMillis());

        assertTrue(state.getLoadAfterLastMeasurement() == state.getMachineState().getLoad());

        assertEquals("db load level is high or exceptionWhileMeasuring", state.getDesc(false));
        assertEquals("db ok and used", state.getDesc(true));
    }

    @Test
    public void shouldUpdate2() throws InterruptedException {
        // given:
        int backOffMultiplier = 3;
        int periodWhenUnused = 2;
        State s1 = createInitialLocalState(periodWhenUnused, backOffMultiplier);
        State s2;
        State s3;

        // when:
        s1.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, medium1);
        s2 = s1.clone();
        s2.updateLocalStateWithNewExecTimes(emptyLogPrefix, medium2, State.notMeasuredInThisCycle);
        s3 = s2.clone();
        s3.updateLocalStateWithNewExecTimes(emptyLogPrefix, high2, high1);

        // then:
        assertEquals("db ok and used", s1.getDesc(false));
        assertEquals("db repaired but still not used", s1.getDesc(true));
        assertFalse(s1.getMachineState().isFailoverActive());
        assertEquals(0, s1.getCycleModuloPeriod(false));
        assertEquals(normalPeriod, s1.getPeriod(false));
        assertEquals(1, s1.getCycleModuloPeriod(true));
        assertEquals(periodWhenUnused, s1.getPeriod(true));

        assertEquals("db ok and used", s2.getDesc(false));
        assertEquals("db repaired but still not used", s2.getDesc(true));
        assertFalse(s1.getMachineState().isFailoverActive());
        assertEquals(0, s2.getCycleModuloPeriod(false));
        assertEquals(normalPeriod, s2.getPeriod(false));
        assertEquals(0, s2.getCycleModuloPeriod(true));
        assertEquals(periodWhenUnused, s2.getPeriod(true));

        assertEquals("db load level is high or exceptionWhileMeasuring", s3.getDesc(false));
        assertEquals("db load level is high or exceptionWhileMeasuring", s3.getDesc(true));
        assertTrue(s3.getMachineState().isFailoverActive());
        assertEquals(1, s3.getCycleModuloPeriod(false));
        assertEquals(backOffMultiplier, s3.getPeriod(false));
        assertEquals(1, s3.getCycleModuloPeriod(true));
        assertEquals(periodWhenUnused * backOffMultiplier, s3.getPeriod(true));
    }

    @Test
    public void shouldBeInInitialState() {
        // given:
        State s;

        // when:
        s = createInitialLocalState(somePeriodWhenUnused, 1);

        // then:
        assertState(s, true, true, false, normalPeriod, normalPeriod, 0, 0);
        assertEquals("db load not measured yet", s.getDesc(false));
        assertEquals("db load not measured yet", s.getDesc(true));
    }

    @Test
    public void shouldDecreaseLoadOfFailoverDatabaseIfUnusedAfterFirstMeasurement() {
        // given:
        int periodWhenUnused = 2;
        State s = createInitialLocalState(periodWhenUnused, 1);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, low1);

        // then:
        assertState(s, true, false, false, normalPeriod, periodWhenUnused, 0, 1);
    }

    @Test
    public void shouldKeepDecreasedLoadOfFailoverDatabaseIfStillUnused() {
        // given:
        State s = createInitialLocalState(4, 1);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, low1);

        // then:
        assertEquals(false, s.sqlTimeIsMeasuredInThisCycle(true));

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, State.notMeasuredInThisCycle);

        // then:
        assertDecreasedLoadOfFailoverDatabase(s, 2);
        assertEquals(false, s.sqlTimeIsMeasuredInThisCycle(true));

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, State.notMeasuredInThisCycle);

        // then:
        assertDecreasedLoadOfFailoverDatabase(s, 3);
    }

    @Test
    public void shouldDecreaseLoadOfFailoverDatabaseIfProblemsAfterNotUnused() {
        // given:
        int periodWhenUnused = 2;
        int backOffMultiplier = 3;
        State s = createInitialLocalState(periodWhenUnused, backOffMultiplier);
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, low1);
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, State.notMeasuredInThisCycle);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, low1, ExceptionEnum.connException.value());

        // then:
        assertEquals(periodWhenUnused * backOffMultiplier, s.getPeriod(true));
    }

    private void assertDecreasedLoadOfFailoverDatabase(State s, int cyclePerPeriodForFailoverDb) {
        assertState(s, true, false, false, normalPeriod, periodWhenUnused, 0, cyclePerPeriodForFailoverDb);
    }

    @Test
    public void shouldDecreaseLoadOfMainDatabaseIfUnusedAfterFirstMeasurement() {
        // given:
        int periodWhenUnused = 2;
        int backOffMultiplier = 3;
        State s = createInitialLocalState(periodWhenUnused, backOffMultiplier);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, high1, low1);

        // then:
        assertState(s, false, true, true, backOffMultiplier, normalPeriod, 1, 0);
    }

    @Test
    public void shouldStartToMonitorFailoverDbThatWasNotUsedIfMainDbBecomesNotOk() {
        // given:
        int periodWhenUnused = 5;
        int backOffMultiplier = 3;
        State s = createInitialLocalState(periodWhenUnused, backOffMultiplier);

        // when both dbs are ok:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, medium1, low1);

        // then:
        assertState(s, true, false, false, normalPeriod, periodWhenUnused, 0, 1);

        // when main db becomes not ok:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, high1, State.notMeasuredInThisCycle);

        // then:
        assertState(s, false, true, true, backOffMultiplier, normalPeriod, 1, 0);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, low2);
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, low2);

        // then:
        assertState(s, true, true, true, backOffMultiplier, normalPeriod, 0, 0);

        // when main db becomes ok but not low:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, medium1, low2);

        // then:
        assertState(s, false, true, true, periodWhenUnused, normalPeriod, 1, 0);

        // when failover db becomes high:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, high1);

        // then:
        assertState(s, true, false, false, normalPeriod, backOffMultiplier, 0, 1);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, high2, State.notMeasuredInThisCycle);

        // then:
        assertState(s, false, false, true, backOffMultiplier, backOffMultiplier, 1, 2);

        // when
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, State.notMeasuredInThisCycle);

        // then:
        assertState(s, false, true, true, backOffMultiplier, backOffMultiplier, 2, 0);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, high2);

        // then:
        assertState(s, true, false, false, backOffMultiplier, backOffMultiplier * backOffMultiplier, 0, 1);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, medium1, State.notMeasuredInThisCycle);

        // then:
        assertState(s, true, false, false, normalPeriod, backOffMultiplier * backOffMultiplier, 0, 2);
    }

    @Test
    public void shouldKeepFailoverUntilMainDbIsLow() {
        // given:
        int periodWhenUnused = 5;
        int backOffMultiplier = 3;
        State s = createInitialLocalState(periodWhenUnused, backOffMultiplier);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, high1, low1);

        // then:
        assertState(s, false, true, true, backOffMultiplier, normalPeriod, 1, 0);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, low1);
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, low1);
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, medium1, medium2);

        // then:
        assertState(s, false, true, true, periodWhenUnused, normalPeriod, 1, 0);
        assertEquals("db repaired but still not used", s.getDesc(false));

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, low1);

        // then:
        assertState(s, false, true, true, periodWhenUnused, normalPeriod, 2, 0);

        // when:
        s.updateLocalStateWithNewExecTimes(emptyLogPrefix, State.notMeasuredInThisCycle, high1);

        // then:
        assertState(s, true, false, false, normalPeriod, backOffMultiplier, 0, 1);

    }

    private void assertState(State s,
                             boolean mainDbIsMeasuredInThisCycle,
                             boolean failoverDbIsMeasuredInThisCycle,
                             boolean failover,
                             int mainPeriod,
                             int failoverPeriod,
                             int mainCycleModuloPeriod,
                             int failoverCycleModuloPeriod) {
        assertEquals(mainDbIsMeasuredInThisCycle, s.sqlTimeIsMeasuredInThisCycle(mainDb));
        assertEquals(failoverDbIsMeasuredInThisCycle, s.sqlTimeIsMeasuredInThisCycle(failoverDb));
        assertEquals(failover, s.getMachineState().isFailoverActive());
        assertEquals(mainPeriod, s.getPeriod(mainDb));
        assertEquals(failoverPeriod, s.getPeriod(failoverDb));
        assertEquals(mainCycleModuloPeriod, s.getCycleModuloPeriod(mainDb));
        assertEquals(failoverCycleModuloPeriod, s.getCycleModuloPeriod(failoverDb));
    }

    private State createInitialLocalState(int periodWhenUnused, int backOffMultiplier) {
        return new State(
                new SqlTimeBasedLoadFactory(failoverThreshold, failbackThreshold),
                initialHost,
                "repo1",
                1,
                false,
                false,
                periodWhenUnused,
                backOffMultiplier,
                100,
                "MAIN_DB    ",
                "FAILOVER_DB");
    }
}
