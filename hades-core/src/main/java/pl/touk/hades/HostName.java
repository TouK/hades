package pl.touk.hades;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HostName {

    public final static String hostName = getHostName();

    private static String getHostName() {
        try {
            Process p = Runtime.getRuntime().exec("hostname");
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String hostName = r.readLine();
            if (hostName == null || hostName.length() == 0) {
                throw new RuntimeException("null or empty host name");
            }
            return hostName;
        } catch (IOException e) {
            throw new RuntimeException("exception while getting the hostname", e);
        }
    }

    public static String get() {
        return hostName;
    }
}
