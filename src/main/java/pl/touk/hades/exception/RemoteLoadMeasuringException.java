package pl.touk.hades.exception;

public class RemoteLoadMeasuringException extends LoadMeasuringException {

    private String remoteQuartzInstanceId;
    private LocalLoadMeasuringException localLoadMeasuringException;

    public RemoteLoadMeasuringException(String remoteQuartzInstanceId, LocalLoadMeasuringException e) {
        this.remoteQuartzInstanceId = remoteQuartzInstanceId;
        this.localLoadMeasuringException = e;
    }

    public String getRemoteQuartzInstanceId() {
        return remoteQuartzInstanceId;
    }

    public LocalLoadMeasuringException getLocalLoadMeasuringException() {
        return localLoadMeasuringException;
    }
}
