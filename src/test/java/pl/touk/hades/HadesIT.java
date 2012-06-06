package pl.touk.hades;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import pl.touk.hades.sqltimemonitoring.SqlTimeBasedTriggerImpl;

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

public class HadesIT {

    private static final Logger logger = LoggerFactory.getLogger(HadesIT.class);

    private static final String separator = " ";

    private Machine slave;

    private ClassPathXmlApplicationContext context;

    public static void main(String[] args) throws IOException, SchedulerException, InterruptedException, SQLException {
        Machine master = new Machine(Integer.parseInt(args[0]));
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("/integration/smx11_at_js22_8-context.xml");
        try {
            do {
                final ExecutableCmd cmd = master.readCommandFromMaster();
                switch (cmd.getCmdName()) {
                    case ping:
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case configureDsQueryTimeMillis:
                        configureDsQueryTimeMillis(ctx, cmd.getArg(0), Long.parseLong(cmd.getArg(1)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case startQuartz:
                        startQuartzScheduler(ctx, Integer.parseInt(cmd.getArg(0)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case scheduleTrigger:
                        scheduleTrigger(ctx, cmd.getArg(0), cmd.getArg(1), Integer.parseInt(cmd.getArg(2)));
                        master.sendResultToMaster(cmd.succeed());
                        break;
                    case stop:
                        master.sendResultToMaster(cmd.succeed());
                        return;
                    default:
                        master.sendResultToMaster(cmd.fail("unknown command: " + cmd.getCmdName() + "; exiting..."));
                        return;
                }
            } while (true);
        } finally {
            ctx.stop();
            master.stop();
        }
    }

    @Before
    public void before() throws IOException {
        context = new ClassPathXmlApplicationContext("/integration/smx11_at_js22_7-context.xml");
        slave = runMachineSimulatorOnSeparateJvm(false);
    }

    @After
    public void after() throws IOException {
        slave.stop();
    }

    @Test
    public void hadesAlfaOnJs227ShouldBorrowPositiveResultFromHadesBetaOnJs228() throws IOException, InterruptedException, SchedulerException, SQLException {
        // given:
        configureDsQueryTimeMillis("DHLITE", unimportantQueryTimeMillis());
        configureDsQueryTimeMillis("DH", throwExceptionIfQueried());
        int queryTimeMillis = 246;
        slave.configureDsQueryTimeMillis("DH", queryTimeMillis);
        slave.configureDsQueryTimeMillis("NASA", unimportantQueryTimeMillis());
        SqlTimeBasedTriggerImpl hadesA_on_smx11_at_js22_7_trigger = context.getBean("hadesA_on_smx11_at_js22_7_trigger", SqlTimeBasedTriggerImpl.class);

        // when:
        slave.scheduleTrigger("hadesA_on_smx11_at_js22_8", "hadesA_on_smx11_at_js22_8_trigger", unreachableTimeInFuture());
        slave.scheduleTrigger("hadesB_on_smx11_at_js22_8", "hadesB_on_smx11_at_js22_8_trigger", 1);
        slave.startQuartzScheduler(2);

        scheduleTrigger("hadesA_on_smx11_at_js22_7", "hadesA_on_smx11_at_js22_7_trigger", 1);
        scheduleTrigger("hadesB_on_smx11_at_js22_7", "hadesB_on_smx11_at_js22_7_trigger", unreachableTimeInFuture());
        startQuartzScheduler(2);

        // then:
        assertTrue(queryTimeMillis <= hadesA_on_smx11_at_js22_7_trigger.getLastFailoverQueryTimeMillis());
        assertTrue(queryTimeMillis + 10 >= hadesA_on_smx11_at_js22_7_trigger.getLastFailoverQueryTimeMillis());
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

    private void configureDsQueryTimeMillis(String dsName, final long queryTime) throws SQLException {
        configureDsQueryTimeMillis(context, dsName, queryTime);
    }

    private static void configureDsQueryTimeMillis(ApplicationContext ctx, final String dsName, final long queryTime) throws SQLException {
        DataSource dsMock = ctx.getBean(dsName, DataSource.class);
        Connection connectionMock = mock(Connection.class);
        PreparedStatement statementMock = mock(PreparedStatement.class);

        when(dsMock.getConnection()).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(statementMock.execute()).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (queryTime != -1) {
                    Thread.sleep(queryTime);
                } else {
                    fail("data source " + dsName + " should not be queried");
                }
                return null;
            }
        });
    }

    private void startQuartzScheduler(int duration) throws InterruptedException, SchedulerException {
        startQuartzScheduler(context, duration);
    }

    private static void startQuartzScheduler(ClassPathXmlApplicationContext ctx, int duration) throws InterruptedException, SchedulerException {
        Scheduler scheduler = ctx.getBean("scheduler", Scheduler.class);
        scheduler.start();
        Thread.sleep(1000L * duration);
        scheduler.shutdown();
    }

    private static void startSpring(ClassPathXmlApplicationContext ctx, int duration) throws InterruptedException {
        ctx.start();
        Thread.sleep(1000L * duration);
        ctx.stop();
    }

    private void scheduleTrigger(String hadesName, String triggerName, int secondsFromNow) {
        scheduleTrigger(context, hadesName, triggerName, secondsFromNow);
    }

    private static void scheduleTrigger(ApplicationContext ctx, String hadesName, String triggerName, int secondsFromNow) {
        SqlTimeBasedTriggerImpl t = ctx.getBean(triggerName, SqlTimeBasedTriggerImpl.class);
        Calendar c = new GregorianCalendar();
        c.add(Calendar.SECOND, secondsFromNow);
        t.setCron(c.get(Calendar.SECOND) + " " + c.get(Calendar.MINUTE) + " " + c.get(Calendar.HOUR_OF_DAY) + " * * ?");
        ctx.getBean(hadesName, Hades.class).init();
    }

    private Machine runMachineSimulatorOnSeparateJvm(boolean debugSuspend) throws IOException {
        ServerSocket masterSocket = new ServerSocket(0);
        ProcessBuilder builder = new ProcessBuilder(Arrays.asList("java", "-Dlog4j.debug=true", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=" + (debugSuspend ? "y" : "n") + ",address=3579", "-Dlog4j.configuration=integration/log4j_js22_8.properties", "-cp", System.getProperty("java.class.path"), "pl.touk.hades.HadesIT", Integer.toString(masterSocket.getLocalPort())));
        Machine machine = new Machine(builder.start(), masterSocket);
        assertEquals("pingResult", machine.instructSlave(CmdName.ping));
        return machine;
    }

    private static class Machine {

        private Process slaveProcess;
        ServerSocket masterSocket;
        Socket remoteSocket;
        private BufferedReader r;
        private BufferedWriter w;

        private Machine(Process slaveProcess, ServerSocket masterSocket) throws IOException {
            this.slaveProcess = slaveProcess;
            this.masterSocket = masterSocket;
            this.remoteSocket = masterSocket.accept();
            this.r = new BufferedReader(new InputStreamReader(remoteSocket.getInputStream()));
            this.w = new BufferedWriter(new OutputStreamWriter(remoteSocket.getOutputStream()));
        }

        private Machine(int masterPort) throws IOException {
            slaveProcess = null;
            masterSocket = null;
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
            return r.readLine();
        }

        public String instructSlave(CmdName cmdName) throws IOException {
            return instructSlave(new Cmd(cmdName));
        }

        public String instructSlave(Cmd cmd) throws IOException {
            logger.info("sending command to slave: " + cmd.toString());
            String s = sendAndRead(cmd.toString());
            logger.info("sent command to slave: " + cmd.toString() + "; result: " + s);
            return s;
        }

        public ExecutableCmd readCommandFromMaster() throws IOException {
            logger.info("reading command from master...");
            ExecutableCmd executableCmd = new ExecutableCmd(read());
            logger.info("read command from master: " + executableCmd.toString());
            return executableCmd;
        }

        public void sendResultToMaster(ExecutableCmd cmd) throws IOException {
            send(cmd.getReply());
            logger.info("sent result to master: " + cmd.getReply());
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

        public void startQuartzScheduler(int forHowLong) throws IOException {
            instructSlave(CmdName.startQuartz.withArgs(Integer.toString(forHowLong)));
        }

        public void scheduleTrigger(String hadesName, String triggerName, int secondsFromNow) throws IOException {
            instructSlave(CmdName.scheduleTrigger.withArgs(hadesName, triggerName, Integer.toString(secondsFromNow)));
        }

        public void configureDsQueryTimeMillis(String ds, long queryTimeMillis) throws IOException {
            instructSlave(CmdName.configureDsQueryTimeMillis.withArgs(ds, Long.toString(queryTimeMillis)));
        }
    }

    private static enum CmdName {
        ping,
//        startSpring,
        startQuartz,
        scheduleTrigger,
        configureDsQueryTimeMillis,
        stop;

        public Cmd withArgs(String... args) {
            return new Cmd(this, args);
        }
    }

    private static class Cmd {

        private CmdName cmdName;
        private List<String> args;

        public Cmd(CmdName cmdName) {
            this.cmdName = cmdName;
        }

        public Cmd(CmdName cmdName, List<String> args) {
            this.cmdName = cmdName;
            this.args = new ArrayList<String>(args);
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

        @Override
        public String toString() {
            return cmdName.name() + listToString(args, separator, true);
        }
    }

    private static class ExecutableCmd extends Cmd {

        private boolean ended = false;
        private boolean failed;
        private List<String> results;

        public ExecutableCmd(String cmdWithArgs) {
            super(extractCmdName(cmdWithArgs), extractArgs(cmdWithArgs));
        }

        private static CmdName extractCmdName(String cmdWithArgs) {
            if (cmdWithArgs != null && cmdWithArgs.length() > 0) {
                String[] array = cmdWithArgs.split(separator);
                if (array.length > 0 && array[0].length() > 0) {
                    return CmdName.valueOf(array[0]);
                } else {
                    throw new IllegalArgumentException("no command");
                }
            } else {
                throw new IllegalArgumentException("cmdWithArgs must not be null or empty");
            }
        }

        private static List<String> extractArgs(String cmdWithArgs) {
            if (cmdWithArgs != null) {
                String[] array = cmdWithArgs.split(separator);
                if (array.length > 1) {
                    List<String> l = new ArrayList<String>(Arrays.asList(array));
                    return l.subList(1, l.size());
                }
            }
            return new ArrayList<String>();
        }

        public String getReply() {
            return getCmdName() + (failed ? "Failure" : "Result") + resultsToString();
        }

        private String resultsToString() {
            if (ended) {
                return listToString(results, separator, true);
            } else {
                throw new IllegalStateException("cmd did not ended yet");
            }
        }

        public ExecutableCmd fail() {
            return fail(null);
        }

        public ExecutableCmd fail(String reason) {
            end(true);
            results = reason != null && reason.length() > 0 ? new ArrayList<String>(Arrays.asList(reason)) : new ArrayList<String>();
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
            return succeed((String[]) null);
        }

        public ExecutableCmd succeed(String... replyArgs) {
            end(false);
            this.results = replyArgs != null && replyArgs.length > 0 ? new ArrayList<String>(Arrays.asList(replyArgs)) : new ArrayList<String>();
            return this;
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
