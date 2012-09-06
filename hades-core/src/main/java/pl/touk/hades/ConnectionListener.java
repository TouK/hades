package pl.touk.hades;

public interface ConnectionListener {

    /**
     * Used by a hades to inform this listener what is the result of every connection request.
     *
     * @param success true if hades successfully got a connection from one of underlying data sources; false otherwise
     * @param failover true if hades attempted to get the connection from the failover data source; false otherwise
     * @param timeNanos how long hades waited for the connection from one of underlying date sources in case of success;
     *                  or how long hades waited before an exception was thrown while getting the connection
     */
    void connectionRequestedFromHades(boolean success, boolean failover, long timeNanos);
}
