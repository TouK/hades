package pl.touk.hades.sql.timemonitoring;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import pl.touk.hades.StringFactory;
import pl.touk.hades.Utils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.Boolean;
import java.lang.Exception;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.Integer;
import java.lang.InterruptedException;
import java.lang.Long;
import java.lang.Object;
import java.lang.Override;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.lang.Runnable;
import java.lang.RuntimeException;
import java.lang.String;
import java.lang.StringBuilder;
import java.lang.System;
import java.lang.Thread;
import java.lang.Throwable;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HadesIT {

    private static final Logger logger = LoggerFactory.getLogger(HadesIT.class);

    private static final String separator = " ";

    private Machine host1;
    private Machine host2;

    public static void main(String[] args) throws IOException, SchedulerException, InterruptedException, SQLException {
        String slaveName = args[2];
        Machine master = new Machine(Integer.parseInt(args[0]), slaveName);
        ClassPathXmlApplicationContext ctx = null;
        ExecutableCmd cmd = null;
        try {
            do {
                cmd = master.readCommandFromMaster();
                switch (cmd.getCmdName()) {
                    case ping:
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case startSpring:
                        ctx = new ClassPathXmlApplicationContext(args[1]);
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case configureDsQueryTimesMillis:
                        for (int i = 0; i < cmd.argCount(); i += 2) {
                            configureDsQueryTimeMillis(ctx, cmd.getArg(i), Long.parseLong(cmd.getArg(i + 1)));
                        }
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case startMonitors:
                        ctx.getBean("monitorAlfa");
                        ctx.getBean("monitorBeta");
                        ctx.getBean("scheduler", Scheduler.class).start();
                        runQuartzSchedulerForGivenTime(ctx, Integer.parseInt(cmd.getArg(0)), java.lang.Boolean.parseBoolean(cmd.getArg(1)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case prepareOneShotCronForMonitor:
                        prepareOneShotCronForMonitor(cmd.getArg(0), Integer.parseInt(cmd.getArg(1)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case prepareOneShotCronsForMonitors:
                        for (int i = 0; i < cmd.argCount(); i += 2) {
                            prepareOneShotCronForMonitor(cmd.getArg(i), Integer.parseInt(cmd.getArg(i + 1)));
                        }
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case scheduleTriggerPeriodically:
                        scheduleTriggerPeriodically(cmd.getArg(0), Integer.parseInt(cmd.getArg(1)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case getLastQueryTimeMillis:
                        long lastQueryTimeMillis = getLastQueryTimeMillis(ctx, Boolean.parseBoolean(cmd.getArg(0)), cmd.getArg(1));
                        master.sendResultToMaster(cmd.succeed(Long.toString(lastQueryTimeMillis)));
                        break;
                    case stop:
                        master.sendResultToMaster(cmd.succeed());
                        logger.info(slaveName + " is about to exit...");
                        return;
                    default:
                        master.sendResultToMaster(cmd.fail("unknown command: " + cmd.getCmdName() + "; exiting..."));
                        return;
                }
                cmd = null;
            } while (true);
        } catch (RuntimeException e) {
            try {
                if (cmd != null) {
                    logger.error("cmd " + cmd.getCmdName(), e);
                    if (!cmd.isReplied()) {
                        master.sendResultToMaster(cmd.fail(e.getClass().getName(), ": ", e.getMessage()));
                    } else {
                        logger.error("exception occurred but reply for command " + cmd + " already sent to master hence master can't be informed", e);
                    }
                } else {
                    logger.error("exception occurred but no command has been read from master hence master can't be informed", e);
                }
            } catch (RuntimeException re) {
                logger.error("runtime exception", re);
            }
        } finally {
            logger.info(slaveName + " is exiting...");
            try {
                master.stop();
                logger.info(slaveName + " closed sockets");
            } catch (Exception e) {
                logger.info(slaveName + " exited with exception\n\n\n", e);
            } finally {
                logger.info(slaveName + " exited\n\n\n");
            }
        }
    }

    @Before
    public void before() throws IOException {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("/integration/quartz_db_init.xml");
        ctx.close();
//        host1 = runMachineSimulatorOnSeparateJvm(3850, true, "host1", "integration/log4j_host1.properties", "/integration/context.xml");
        host1 = runMachineSimulatorOnSeparateJvm(-1, true, "host1", "integration/log4j_host1.properties", "/integration/context.xml");
        host2 = runMachineSimulatorOnSeparateJvm(-1, true, "host2", "integration/log4j_host2.properties", "/integration/context.xml");
    }

    @After
    public void after() throws IOException, InterruptedException {
        host1.stop();
        host2.stop();
        logger.info("waiting for " + host1.slaveName + " to exit...");
        host1.slaveProcess.waitFor();
        logger.info(host1.slaveName + " exited");
        logger.info("waiting for " + host2.slaveName + " to exit...");
        host2.slaveProcess.waitFor();
        logger.info(host2.slaveName + " exited");
    }

    @Test
    public void hadesAlfaOnHost1ShouldBorrowPositiveResultForDhFromHadesBetaOnHost2() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        int queryTimeMillis = 246;
        long lastQueryTimeMillis;

        // when:
        host1.startSpring();
        host2.startSpring();

        host1.configureDsQueryTimesMillis(
                "DHLITE", Long.toString(unimportantQueryTimeMillis()),
                "DH", Long.toString(throwExceptionIfQueried()));
        host2.configureDsQueryTimesMillis(
                "DH", Long.toString(queryTimeMillis),
                "NASA", Long.toString(unimportantQueryTimeMillis()));

        host2.prepareOneShotCronsForMonitors(
                "cronAlfa", Integer.toString(unreachableTimeInFuture()),
                "cronBeta", "3");
        host2.startMonitors(4, false);

        host1.prepareOneShotCronsForMonitors(
                "cronBeta", Integer.toString(unreachableTimeInFuture()),
                "cronAlfa", "3");
        host1.startMonitors(4, false);

        // then:
        lastQueryTimeMillis = host1.getLastQueryTimeMillis(false, "monitorAlfa");
        assertTrue("queryTimeMillis: " + queryTimeMillis + ", lastQueryTimeMillis: " + lastQueryTimeMillis, queryTimeMillis <= lastQueryTimeMillis + 2);
        assertTrue("queryTimeMillis: " + queryTimeMillis + ", lastQueryTimeMillis: " + lastQueryTimeMillis, queryTimeMillis + 30 >= lastQueryTimeMillis);
    }

    @Test
    public void hadesBetaOnHost2ShouldBorrowPositiveResultForDhFromHadesAlfaOnHost1() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        long queryTimeMillis = 219;
        long lastQueryTimeMillis;

        // when:
        host1.startSpring();
        host2.startSpring();

        host1.configureDsQueryTimesMillis(
                "DH", Long.toString(queryTimeMillis),
                "DHLITE", Long.toString(unimportantQueryTimeMillis()));
        host2.configureDsQueryTimesMillis(
                "NASA", Long.toString(unimportantQueryTimeMillis()),
                "DH", Long.toString(throwExceptionIfQueried()));

        host1.prepareOneShotCronsForMonitors(
                "cronBeta", Long.toString(unreachableTimeInFuture()),
                "cronAlfa", "3");
        host1.startMonitors(4, false);

        host2.prepareOneShotCronsForMonitors(
                "cronAlfa", Long.toString(unreachableTimeInFuture()),
                "cronBeta", "3");
        host2.startMonitors(4, false);

        // then:
        lastQueryTimeMillis = host2.getLastQueryTimeMillis(false, "monitorBeta");
        assertTrue("queryTimeMillis: " + queryTimeMillis + ", lastQueryTimeMillis: " + lastQueryTimeMillis, queryTimeMillis <= lastQueryTimeMillis + 2);
        assertTrue("queryTimeMillis: " + queryTimeMillis + ", lastQueryTimeMillis: " + lastQueryTimeMillis, queryTimeMillis + 30 >= lastQueryTimeMillis);
    }

    @Test
    public void hadesAlfaOnHost1ShouldInsertResultForDh() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        int queryTimeMillis = 152;

        // when:
        host1.startSpring();
        host1.preparePeriodicCronForMonitor("cronAlfa", 2);
        host1.prepareOneShotCronForMonitor("cronBeta", unreachableTimeInFuture());
        host1.configureDsQueryTimesMillis(
                "DHLITE", Long.toString(unimportantQueryTimeMillis()),
                "DH", Long.toString(queryTimeMillis));
        host1.startMonitors(5, false);

        // then:
    }

    @Test
    public void hadesAlfaOnHost1ShouldBeReplacedByHadesAlfaOnHost2WhenHost1Stops() throws IOException, InterruptedException, SchedulerException, SQLException {
        // when:
        host1.startSpring();
        host2.startSpring();

        host1.preparePeriodicCronForMonitor("cronAlfa", 3);
        host1.prepareOneShotCronForMonitor("cronBeta", unreachableTimeInFuture());
        host2.preparePeriodicCronForMonitor("cronAlfa", 3);
        host2.prepareOneShotCronForMonitor("cronBeta", unreachableTimeInFuture());

        host1.configureDsQueryTimesMillis(
                "DHLITE", "1",
                "DH", "3",
                "NASA", Long.toString(throwExceptionIfQueried()));
        host2.configureDsQueryTimesMillis(
                "DHLITE", "5",
                "DH", "7",
                "NASA", Long.toString(throwExceptionIfQueried()));

        host1.startMonitors(15, true);
        host2.startMonitors(25, false);

        // then:
    }

    private int unreachableTimeInFuture() {
        return 3600 * 100;
    }

    private long throwExceptionIfQueried() {
        return -1;
    }

    private long unimportantQueryTimeMillis() {
        return 0;
    }

    private static void configureDsQueryTimeMillis(ApplicationContext ctx, final String dsName, final long queryTimeMillis) throws SQLException {
        DataSource dsMock = ctx.getBean(dsName, DataSource.class);
        Connection connectionMock = mock(Connection.class);
        PreparedStatement statementMock = mock(PreparedStatement.class);

        when(dsMock.getConnection()).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(statementMock.execute()).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (queryTimeMillis != -1) {
                    if (queryTimeMillis > 0) {
                        Thread.sleep(queryTimeMillis);
                    }
                } else {
                    fail("data source " + dsName + " should not be queried");
                }
                return null;
            }
        });
    }

    private static void runQuartzSchedulerForGivenTime(ClassPathXmlApplicationContext ctx, final int duration, boolean async) throws SchedulerException {
        final Scheduler scheduler = ctx.getBean("scheduler", Scheduler.class);
        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    SECONDS.sleep(duration);
                    scheduler.shutdown();
                } catch (Exception e) {
                    logger.error("failed to shutdown scheduler", e);
                }
            }
        };
        if (async) {
            new Thread(runnable).start();
        } else {
            runnable.run();
        }
    }

    private static void startSpring(ClassPathXmlApplicationContext ctx, int duration) throws InterruptedException {
        ctx.start();
        SECONDS.sleep(duration);
        ctx.stop();
    }

    private static void prepareOneShotCronForMonitor(String cronName, int secondsFromNow) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.SECOND, secondsFromNow);
        Calendar c2 = Calendar.getInstance();
        c2.setTime(new Date(Utils.roundMillisWithSecondPrecision(c.getTime().getTime())));
        StringFactory.beansByName.put(cronName, c2.get(Calendar.SECOND) + " " + c.get(Calendar.MINUTE) + " " + c.get(Calendar.HOUR_OF_DAY) + " * * ?");
    }

    private static void scheduleTriggerPeriodically(String cronName, int period) {
        StringFactory.beansByName.put(cronName, "0/" + period + " * * * * ?");
    }

    private static long getLastQueryTimeMillis(ApplicationContext ctx, boolean main, String monitorName) {
        SqlTimeBasedQuartzMonitor monitor = ctx.getBean(monitorName, SqlTimeBasedQuartzMonitor.class);
        return monitor.getLastQueryTimeMillis(main);
    }

    private Machine runMachineSimulatorOnSeparateJvm(int debugPort, boolean debugSuspend, String name, String log4jConfiguration, String context) throws IOException {
        ServerSocket masterSocket = new ServerSocket(0);
        ProcessBuilder builder;
        if (debugPort > 0) {
            builder = new ProcessBuilder(Arrays.asList("java", "-Dlog4j.debug=true", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=" + (debugSuspend ? "y" : "n") + ",address=" + debugPort, "-Dlog4j.configuration=" + log4jConfiguration, "-cp", System.getProperty("java.class.path"), this.getClass().getName(), Integer.toString(masterSocket.getLocalPort()), context, name));
        } else {
            builder = new ProcessBuilder(Arrays.asList("java", "-Dlog4j.configuration=" + log4jConfiguration, "-cp", System.getProperty("java.class.path"), this.getClass().getName(), Integer.toString(masterSocket.getLocalPort()), context, name));
        }
        Machine machine = new Machine(builder.start(), masterSocket, name);
        machine.instructSlave(CmdName.ping);
        return machine;
    }

    private static class Machine {

        private final Process slaveProcess;
        private final ServerSocket masterSocket;
        private final String slaveName;
        private Socket remoteSocket;
        private BufferedReader r;
        private BufferedWriter w;

        private Machine(Process slaveProcess, ServerSocket masterSocket, String slaveName) throws IOException {
            this.slaveProcess = slaveProcess;
            this.masterSocket = masterSocket;
            this.slaveName = slaveName;
            this.remoteSocket = masterSocket.accept();
            this.r = new BufferedReader(new InputStreamReader(remoteSocket.getInputStream()));
            this.w = new BufferedWriter(new OutputStreamWriter(remoteSocket.getOutputStream()));
        }

        private Machine(int masterPort, String slaveName) throws IOException {
            slaveProcess = null;
            masterSocket = null;
            this.slaveName = slaveName;
            remoteSocket = new Socket("localhost", masterPort);
            r = new BufferedReader(new InputStreamReader(remoteSocket.getInputStream()));
            w = new BufferedWriter(new OutputStreamWriter(remoteSocket.getOutputStream()));
        }

        public String sendAndRead(String... strings) throws IOException {
            send(strings);
            return read();
        }

        public void send(String... strings) throws IOException {
            if (strings != null && strings.length > 0) {
                w.write(strings[0]);
                for (int i = 1; i < strings.length; i++) {
                    w.write(separator);
                    w.write(strings[i]);
                }
                w.newLine();
                w.flush();
            }
        }

        public String read() throws IOException {
            String s = r.readLine();
            if (s == null) {
                throw new IllegalStateException("readLine returned null");
            }
            return s;
        }

        public ExecutableCmd instructSlave(CmdName cmdName) throws IOException {
            return instructSlave(new Cmd(cmdName));
        }

        public ExecutableCmd instructSlave(Cmd cmd) throws IOException {
            logger.info("sending command to " + slaveName + ": " + cmd.toString());
            String s = sendAndRead(cmd.toString());
            logger.info("sent command to " + slaveName + ": " + cmd.toString() + "; result: " + s);
            ExecutableCmd executableCmd = ExecutableCmd.parse(s);
            if (!executableCmd.isEnded()) {
                throw new RuntimeException("invalid answer from " + slaveName + " (not result or failure): " + s);
            }
            return executableCmd;
        }

        public ExecutableCmd readCommandFromMaster() throws IOException {
            logger.info(slaveName + " is reading command from master...");
            ExecutableCmd executableCmd = ExecutableCmd.parse(read());
            logger.info(slaveName + " read command from master: " + executableCmd.toString());
            return executableCmd;
        }

        public void sendResultToMaster(ExecutableCmd cmd) throws IOException {
            String reply = cmd.reply();
            send(reply);
            logger.info(slaveName + " sent result to master: " + reply);
        }

        public void stop() throws IOException {
            try {
                if (masterSocket != null) {
                    instructSlave(CmdName.stop);
                }
            } finally {
                if (slaveProcess != null) {
                    slaveProcess.destroy();
                }
                try {
                    r.close();
                } finally {
                    try {
                        w.close();
                    } finally {
                        try {
                            remoteSocket.close();
                        } finally {
                            if (masterSocket != null) {
                                masterSocket.close();
                            }
                        }
                    }
                }
            }
        }

        public void startMonitors(int forHowLong, boolean async) throws IOException {
            instructSlave(CmdName.startMonitors.withArgs(Integer.toString(forHowLong), Boolean.toString(async)));
        }

        public void prepareOneShotCronForMonitor(String cronName, int secondsFromNow) throws IOException {
            instructSlave(CmdName.prepareOneShotCronForMonitor.withArgs(cronName, Integer.toString(secondsFromNow)));
        }

        public void prepareOneShotCronsForMonitors(String... cronNameSecondsFromNowArray) throws IOException {
            instructSlave(CmdName.prepareOneShotCronsForMonitors.withArgs(cronNameSecondsFromNowArray));
        }

        public void preparePeriodicCronForMonitor(String cronName, int period) throws IOException {
            instructSlave(CmdName.scheduleTriggerPeriodically.withArgs(cronName, Integer.toString(period)));
        }

        public long getLastQueryTimeMillis(boolean main, String monitorName) throws IOException {
            return Long.parseLong(instructSlave(CmdName.getLastQueryTimeMillis.withArgs(Boolean.toString(main), monitorName)).getResultArg(0));
        }

        public void configureDsQueryTimesMillis(String... dsQueryTimeMillisArray) throws IOException {
            instructSlave(CmdName.configureDsQueryTimesMillis.withArgs(dsQueryTimeMillisArray));
        }

        public void startSpring() throws IOException {
            instructSlave(CmdName.startSpring);
        }
    }

    private static enum CmdName {
        ping,
        startMonitors,
        prepareOneShotCronForMonitor,
        prepareOneShotCronsForMonitors,
        scheduleTriggerPeriodically,
        startSpring,
        configureDsQueryTimesMillis,
        stop,
        getLastQueryTimeMillis;

        public Cmd withArgs(String... args) {
            return new Cmd(this, args);
        }
    }

    private static class Cmd {

        private final CmdName cmdName;
        private List<String> args;

        public Cmd(CmdName cmdName) {
            this.cmdName = cmdName;
        }

        public Cmd(CmdName cmdName, List<String> args) {
            this.cmdName = cmdName;
            this.args = args != null ? new ArrayList<String>(args) : new ArrayList<String>();
        }

        public Cmd(CmdName cmdName, String... args) {
            this(cmdName, Arrays.asList(args));
        }

        public CmdName getCmdName() {
            return cmdName;
        }

        public String getArg(int i) {
            return args.get(i);
        }

        public int argCount() {
            return args.size();
        }

        @Override
        public String toString() {
            return cmdName.name() + listToString(args, separator, true);
        }
    }

    private static class ExecutableCmd extends Cmd {

        public static final String resultSuffix = "Result";
        public static final String failureSuffix = "Failure";

        private boolean ended = false;
        private boolean failed;
        private List<String> results;
        private boolean replied = false;

        private ExecutableCmd(CmdName cmd, List<String> cmdArgs, boolean ended, boolean failed, List<String> resultArgs) {
            super(cmd, cmdArgs);
            if (ended) {
                if (failed) {
                    fail(resultArgs);
                } else {
                    succeed(resultArgs);
                }
            }
        }

        private static ExecutableCmd parse(String cmdWithArgs) {
            if (cmdWithArgs != null && cmdWithArgs.length() > 0) {
                String[] array = cmdWithArgs.split(separator);
                if (array.length > 0 && array[0].length() > 0) {
                    if (array[0].endsWith(resultSuffix)) {
                        return new ExecutableCmd(CmdName.valueOf(array[0].substring(0, array[0].length() - resultSuffix.length())), null, true, false, Arrays.asList(array).subList(1, array.length));
                    } else if (array[0].endsWith(failureSuffix)) {
                        return new ExecutableCmd(CmdName.valueOf(array[0].substring(0, array[0].length() - failureSuffix.length())), null, true, true, Arrays.asList(array).subList(1, array.length));
                    } else {
                        return new ExecutableCmd(CmdName.valueOf(array[0]), Arrays.asList(array).subList(1, array.length), false, false, null);
                    }
                } else {
                    throw new IllegalArgumentException("no command");
                }
            } else {
                throw new IllegalArgumentException("cmdWithArgs must not be null or empty");
            }
        }

        public String reply() {
            if (ended) {
                if (!replied) {
                    replied = true;
                    return getCmdName() + (failed ? failureSuffix : resultSuffix) + resultsToString();
                } else {
                    throw new IllegalStateException("reply already sent for command");
                }
            } else {
                throw new IllegalStateException("command did not end yet");
            }
        }

        private String resultsToString() {
            if (ended) {
                return listToString(results, separator, true);
            } else {
                throw new IllegalStateException("cmd did not ended yet");
            }
        }

        public ExecutableCmd fail() {
            return fail((List<String>) null);
        }

        public ExecutableCmd fail(String... reasonArgs) {
            return fail(reasonArgs != null ? new ArrayList<String>(Arrays.asList(reasonArgs)) : new ArrayList<String>());
        }

        private ExecutableCmd fail(List<String> reasonArgs) {
            end(true);
            results = reasonArgs;
            return this;
        }

        private void end(boolean failed) {
            if (!ended) {
                ended = true;
                this.failed = failed;
            } else {
                throw new IllegalStateException("already ended");
            }
        }

        public ExecutableCmd succeed() {
            return succeed((List<String>) null);
        }

        public ExecutableCmd succeed(String... resultArgs) {
            return succeed(resultArgs != null && resultArgs.length > 0 ? new ArrayList<String>(Arrays.asList(resultArgs)) : new ArrayList<String>());
        }

        public ExecutableCmd succeed(List<String> resultArgs) {
            end(false);
            this.results = resultArgs;
            return this;
        }

        public boolean isEnded() {
            return ended;
        }

        public String getResultArg(int i) {
            return this.results.get(i);
        }

        public boolean isReplied() {
            return replied;
        }
    }

    private static String listToString(List<String> l, String separator, boolean prependSeparator) {
        if (l != null && l.size() > 0) {
            StringBuilder sb = new StringBuilder(l.get(0));
            for (int i = 1; i < l.size(); i++) {
                sb.append(separator).append(l.get(i));
            }
            return (prependSeparator && sb.length() > 0 ? separator : "") + sb.toString();
        } else {
            return "";
        }
    }
}
