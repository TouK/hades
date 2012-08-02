package pl.touk.hades.sql.timemonitoring;

import pl.touk.hades.exception.LoadMeasuringException;
import pl.touk.hades.exception.UnexpectedException;
import pl.touk.hades.sql.exception.*;

public enum ExceptionEnum {
    loadMeasuringException         {public LoadMeasuringException createException(String logPrefix) { return new LoadMeasuringException         (logPrefix); }},
    unexpectedException            {public LoadMeasuringException createException(String logPrefix) { return new UnexpectedException            (logPrefix); }},
    connException                  {public LoadMeasuringException createException(String logPrefix) { return new ConnException                  (logPrefix); }},
    connTimeout                    {public LoadMeasuringException createException(String logPrefix) { return new ConnTimeout                    (logPrefix); }},
    prepareStmtException           {public LoadMeasuringException createException(String logPrefix) { return new PrepareStmtException           (logPrefix); }},
    sqlExecTimeoutSettingException {public LoadMeasuringException createException(String logPrefix) { return new SqlExecTimeoutSettingException (logPrefix); }},
    sqlExecException               {public LoadMeasuringException createException(String logPrefix) { return new SqlExecException               (logPrefix); }},
    sqlExecTimeout                 {public LoadMeasuringException createException(String logPrefix) { return new SqlExecTimeout                 (logPrefix); }};

    private final Class<? extends LoadMeasuringException> exceptionClass = createException(null).getClass();

    abstract protected LoadMeasuringException createException(String logPrefix);

    public long value() {
        return Long.MAX_VALUE - ordinal();
    }

    public static long minErroneousValue() {
        return values()[values().length - 1].value();
    }

    public static String minErroneousValueAsStr() {
        return "Long.MAX_VALUE - " + (values().length - 1);
    }

    public static long valueForException(LoadMeasuringException e) {
        for (ExceptionEnum error: values()) {
            if (error.exceptionClass == e.getClass()) {
                return error.value();
            }
        }
        throw new IllegalArgumentException("unknown class for LoadMeasuringException: " + e.getClass());
    }

    public static String erroneousValuesAsStr(long time) {
        if (time < minErroneousValue()) {
            return Long.toString(time);
        } else {
            return values()[(int)(Long.MAX_VALUE - time)].name();
        }
    }
}
