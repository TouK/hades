package pl.touk.hades;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.hades.sql.exception.PrepareStmtException;
import pl.touk.hades.sql.exception.SqlExecTimeoutSettingException;
import pl.touk.hades.sql.timemonitoring.ExceptionEnum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    private static final long nanosInMillisecond = 1000000L;
    private static final DecimalFormat millisecondFormat = new DecimalFormat("#0.000 ms");

    public static final DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss,SSS");
    public static final DateFormat tf = new SimpleDateFormat("HH:mm:ss");

    private static final String indentation = "  ";

    private Utils() {
    }

    public static String indent(String s) {
        return s + indentation;
    }

    public static String format(Date d) {
        return df.format(d);
    }

    public static String formatTime(Date d) {
        return tf.format(d);
    }

    public static PreparedStatement safelyPrepareStatement(String curRunLogPrefix, Connection c, String dsDesc, int sqlExecTimeout, String sql) throws PrepareStmtException, SqlExecTimeoutSettingException {
        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement(sql);
            if (sqlExecTimeout >= 0) {
                ps.setQueryTimeout(sqlExecTimeout);
            }
            return ps;
        } catch (Exception e) {
            try {
                logger.error(curRunLogPrefix + "exception while " + (ps == null ? "preparing statement" : "setting query timeout") + " for " + dsDesc, e);
                if (ps == null) {
                    throw new PrepareStmtException(curRunLogPrefix);
                } else {
                    throw new SqlExecTimeoutSettingException(curRunLogPrefix);
                }
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e1) {
                        logger.error(curRunLogPrefix + "exception while closing prepared statement after failure in setting query timeout for " + dsDesc, e);
                    }
                }
            }
        }
    }

    public static long nanosToMillis(long l) {
        return Math.round(((double) l) / nanosInMillisecond);
    }

    public static long roundMillisWithSecondPrecision(long l) {
        return Math.round(((double) l) / 1000) * 1000;
    }

    public static String nanosToMillisAsStr(long l) {
        if (l < ExceptionEnum.minErroneousValue()) {
            return millisecondFormat.format(((double) l) / nanosInMillisecond);
        } else {
            return "<N/A because of exception(warning) - assuming infinity>";
        }
    }

    public static String assertNonEmpty(String s, String name) {
        if (s == null || s.length() == 0) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
        return s;
    }

    public static void assertNotNull(Object field, String fieldName) {
        if (field == null) {
            throw new IllegalArgumentException("null " + fieldName);
        }
    }

    public static void assertNull(Object field, String fieldName) {
        if (field != null) {
            throw new IllegalStateException("non-null " + fieldName);
        }
    }

    public static void assertSame(Object o1, Object o2, String msg) {
        if (o1 != o2) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void assertNonNegative(int field, String fieldName) {
        if (field < 0) {
            throw new IllegalArgumentException(fieldName + " must be equal to or greater than zero");
        }
    }

    public static void assertNonNegative(long field, String fieldName) {
        if (field < 0) {
            throw new IllegalArgumentException(fieldName + " must be equal to or greater than zero");
        }
    }

    public static void assertPositive(int field, String fieldName) {
        if (field <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero");
        }
    }

    public static long millisToNanos(int i) {
        return i * nanosInMillisecond;
    }

    public static void assertNonZero(int i, String fieldName) {
        if (i == 0) {
            throw new IllegalArgumentException(fieldName + " must be non-zero");
        }
    }
}
