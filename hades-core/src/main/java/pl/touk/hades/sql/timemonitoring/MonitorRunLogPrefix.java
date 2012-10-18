package pl.touk.hades.sql.timemonitoring;

/**
 * Text logged as prefix of logged messages. It identifies quartz instance, hades and start time of monitoring method.
 * It also contains context information (what is currently executed).
 */
public class MonitorRunLogPrefix {

    public final static String defaultIndentation = "  ";

    private final String indentation;
    private final String id;
    private final String context;

    private final String idInBrackets;

    public MonitorRunLogPrefix(String id) {
        this(defaultIndentation, id, "");
    }

    public MonitorRunLogPrefix(String indentation, String id) {
        this(indentation, id, "");
    }

    private MonitorRunLogPrefix(String indentation, String id, String context) {
        this.indentation = indentation;
        this.id = id;
        this.idInBrackets = id != null && id.length() > 0 ? "[" + id + "] " : "";
        this.context = context;
    }

    public MonitorRunLogPrefix() {
        this("", "", "");
    }

    public MonitorRunLogPrefix indent() {
        return append(indentation);
    }

    public String toString() {
        return idInBrackets + context;
    }

    public String getId() {
        return id;
    }

    public MonitorRunLogPrefix append(String s) {
        return new MonitorRunLogPrefix(indentation, id, context + s);
    }
}
