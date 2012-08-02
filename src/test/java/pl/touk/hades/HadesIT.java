package pl.touk.hades;

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
import pl.touk.hades.sql.timemonitoring.SqlTimeBasedTriggerImpl;

import javax.sql.DataSource;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HadesIT {

    private static final Logger logger = LoggerFactory.getLogger(HadesIT.class);

    private static final String separator = " ";

    private Machine js227;
    private Machine js228;

    public static void main(String[] args) throws IOException, SchedulerException, InterruptedException, SQLException {
        Machine master = new Machine(Integer.parseInt(args[0]), args[2]);
        ClassPathXmlApplicationContext ctx = null;
        ExecutableCmd cmd = null;
        try {
            do {
                cmd = master.readCommandFromMaster();
                switch (cmd.getCmdName()) {
                    case ping:
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case startSpringAndConfigureDsQueryTimeMillis:
                        ctx = new ClassPathXmlApplicationContext(args[1]);
                        for (int i = 0; i < cmd.argCount(); i += 2) {
                            configureDsQueryTimeMillis(ctx, cmd.getArg(i), Long.parseLong(cmd.getArg(i + 1)));
                        }
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case startQuartz:
                        startQuartzScheduler(ctx, Integer.parseInt(cmd.getArg(0)), Boolean.parseBoolean(cmd.getArg(1)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case scheduleTrigger:
                        scheduleTrigger(cmd.getArg(0), Integer.parseInt(cmd.getArg(1)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case scheduleTriggers:
                        for (int i = 0; i < cmd.argCount(); i += 2) {
                            scheduleTrigger(cmd.getArg(i), Integer.parseInt(cmd.getArg(i + 1)));
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
            if (ctx != null) {
                ctx.stop();
                ctx.destroy();
            }
            master.stop();
        }
    }

    @Before
    public void before() throws IOException {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("/integration/smx11_quartz_db_init.xml");
        ctx.close();
//        js227 = runMachineSimulatorOnSeparateJvm(3850, true, "js22-7", "integration/log4j_js22_7.properties", "/integration/smx11_at_js22_7-context.xml");
        js227 = runMachineSimulatorOnSeparateJvm(-1, true, "js22-7", "integration/log4j_js22_7.properties", "/integration/smx11_at_js22_7-context.xml");
        js228 = runMachineSimulatorOnSeparateJvm(-1, true, "js22-8", "integration/log4j_js22_8.properties", "/integration/smx11_at_js22_8-context.xml");
    }

    @After
    public void after() throws IOException {
        js227.stop();
        js228.stop();
    }

    @Test
    public void hadesAlfaOnJs227ShouldBorrowPositiveResultForDhFromHadesBetaOnJs228() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        int queryTimeMillis = 246;
        long lastQueryTimeMillis;

        // when:
        js228.scheduleTriggers(
                "cronA", Integer.toString(unreachableTimeInFuture()),
                "cronB", "2");
        js228.startSpringAndConfigureDsQueryTimeMillis(
                "DH", Long.toString(queryTimeMillis),
                "NASA", Long.toString(unimportantQueryTimeMillis()));
        js228.startQuartzScheduler(2, false);

        js227.scheduleTriggers(
                "cronB", Integer.toString(unreachableTimeInFuture()),
                "cronA", Integer.toString(2));
        js227.startSpringAndConfigureDsQueryTimeMillis(
                "DHLITE", Long.toString(unimportantQueryTimeMillis()),
                "DH", Long.toString(throwExceptionIfQueried()));
        js227.startQuartzScheduler(2, false);

        // then:
        lastQueryTimeMillis = js227.getLastQueryTimeMillis(false, "triggerA");
        assertTrue("lastQueryTimeMillis: " + lastQueryTimeMillis, queryTimeMillis <= lastQueryTimeMillis);
        assertTrue("lastQueryTimeMillis: " + lastQueryTimeMillis, queryTimeMillis + 10 >= lastQueryTimeMillis);
    }

    @Test
    public void hadesBetaOnJs228ShouldBorrowPositiveResultForDhFromHadesAlfaOnJs227() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        long queryTimeMillis = 219;

        // when:
        js227.scheduleTriggers(
                "cronB", Long.toString(unreachableTimeInFuture()),
                "cronA", "2");
        js227.startSpringAndConfigureDsQueryTimeMillis(
                "DH", Long.toString(queryTimeMillis),
                "DHLITE", Long.toString(unimportantQueryTimeMillis()));
        js227.startQuartzScheduler(2, false);

        js228.scheduleTriggers(
                "cronA", Long.toString(unreachableTimeInFuture()),
                "cronB", "2");
        js228.startSpringAndConfigureDsQueryTimeMillis(
                "NASA", Long.toString(unimportantQueryTimeMillis()),
                "DH", Long.toString(throwExceptionIfQueried()));
        js228.startQuartzScheduler(2, false);

        // then:
        assertTrue(queryTimeMillis <= js228.getLastQueryTimeMillis(false, "triggerB"));
        assertTrue(queryTimeMillis + 10 >= js228.getLastQueryTimeMillis(false, "triggerB"));
    }

    @Test
    public void hadesAlfaOnJs227ShouldInsertResultForDh() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        int queryTimeMillis = 152;

        // when:
        js227.scheduleTriggerPeriodically("cronA", 2);
        js227.scheduleTrigger("cronB", unreachableTimeInFuture());
        js227.startSpringAndConfigureDsQueryTimeMillis(
                "DHLITE", Long.toString(unimportantQueryTimeMillis()),
                "DH", Long.toString(queryTimeMillis));
        js227.startQuartzScheduler(5, false);

        // then:

    }

    @Test
    public void hadesAlfaOnJs227ShouldBeReplacedByHadesAlfaOnJs228WhenJs227Stops() throws IOException, InterruptedException, SchedulerException, SQLException {
        // when:
        js227.scheduleTriggerPeriodically("cronA", 3);
        js227.scheduleTrigger("cronB", unreachableTimeInFuture());
        js228.scheduleTriggerPeriodically("cronA", 3);
        js228.scheduleTrigger("cronB", unreachableTimeInFuture());

        js227.startSpringAndConfigureDsQueryTimeMillis(
                "DHLITE", "1",
                "DH", "3",
                "NASA", Long.toString(throwExceptionIfQueried()));

        js228.startSpringAndConfigureDsQueryTimeMillis(
                "DHLITE", "5",
                "DH", "7",
                "NASA", Long.toString(throwExceptionIfQueried()));

        js227.startQuartzScheduler(15, true);
        js228.startQuartzScheduler(25, false);

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

    private static void startQuartzScheduler(ClassPathXmlApplicationContext ctx, final int duration, boolean async) throws SchedulerException {
        final Scheduler scheduler = ctx.getBean("scheduler", Scheduler.class);
        scheduler.start();
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

    private static void scheduleTrigger(String cronName, int secondsFromNow) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.SECOND, secondsFromNow);
        Calendar c2 = Calendar.getInstance();
        c2.setTime(new Date(Utils.roundMillisWithSecondPrecision(c.getTime().getTime())));
        StringFactory.beansByName.put(cronName, c2.get(Calendar.SECOND) + " " + c.get(Calendar.MINUTE) + " " + c.get(Calendar.HOUR_OF_DAY) + " * * ?");
    }

    private static void scheduleTriggerPeriodically(String cronName, int period) {
        StringFactory.beansByName.put(cronName, "0/" + period + " * * * * ?");
    }

    private static long getLastQueryTimeMillis(ApplicationContext ctx, boolean main, String hadesTriggerName) {
        SqlTimeBasedTriggerImpl hadesTrigger = ctx.getBean(hadesTriggerName, SqlTimeBasedTriggerImpl.class);
        return hadesTrigger.getLastFailoverQueryTimeMillis(main);
    }

    private Machine runMachineSimulatorOnSeparateJvm(int debugPort, boolean debugSuspend, String name, String log4jConfiguration, String context) throws IOException {
        ServerSocket masterSocket = new ServerSocket(0);
        ProcessBuilder builder;
        if (debugPort > 0) {
            builder = new ProcessBuilder(Arrays.asList("java", "-Dlog4j.debug=true", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=" + (debugSuspend ? "y" : "n") + ",address=" + debugPort, "-Dlog4j.configuration=" + log4jConfiguration, "-cp", System.getProperty("java.class.path"), "pl.touk.hades.HadesIT", Integer.toString(masterSocket.getLocalPort()), context, name));
        } else {
            builder = new ProcessBuilder(Arrays.asList("java", "-Dlog4j.configuration=" + log4jConfiguration, "-cp", System.getProperty("java.class.path"), "pl.touk.hades.HadesIT", Integer.toString(masterSocket.getLocalPort()), context, name));
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

        public void startQuartzScheduler(int forHowLong, boolean async) throws IOException {
            instructSlave(CmdName.startQuartz.withArgs(Integer.toString(forHowLong), Boolean.toString(async)));
        }

        public void scheduleTrigger(String cronName, int secondsFromNow) throws IOException {
            instructSlave(CmdName.scheduleTrigger.withArgs(cronName, Integer.toString(secondsFromNow)));
        }

        public void scheduleTriggers(String... cronNameSecondsFromNowArray) throws IOException {
            instructSlave(CmdName.scheduleTriggers.withArgs(cronNameSecondsFromNowArray));
        }

        public void scheduleTriggerPeriodically(String cronName, int period) throws IOException {
            instructSlave(CmdName.scheduleTriggerPeriodically.withArgs(cronName, Integer.toString(period)));
        }

        public void configureDsQueryTimeMillis(String ds, long queryTimeMillis) throws IOException {
            instructSlave(CmdName.startSpringAndConfigureDsQueryTimeMillis.withArgs(ds, Long.toString(queryTimeMillis)));
        }

        public long getLastQueryTimeMillis(boolean main, String hadesTriggerName) throws IOException {
            return Long.parseLong(instructSlave(CmdName.getLastQueryTimeMillis.withArgs(Boolean.toString(main), hadesTriggerName)).getResultArg(0));
        }

        public void startSpringAndConfigureDsQueryTimeMillis(String... dsQueryTimeMillisArray) throws IOException {
            instructSlave(CmdName.startSpringAndConfigureDsQueryTimeMillis.withArgs(dsQueryTimeMillisArray));
        }
    }

    private static enum CmdName {
        ping,
        startQuartz,
        scheduleTrigger,
        scheduleTriggers,
        scheduleTriggerPeriodically,
        startSpringAndConfigureDsQueryTimeMillis,
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
