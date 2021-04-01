package moe.dare.briareus.yarn.sensei;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import java.util.Objects;

public final class ApplicationStatus {
    private final FinalApplicationStatus finalApplicationStatus;
    private final String message;
    private final String newTrackUrl;

    public static ApplicationStatus failed() {
        return new ApplicationStatus(FinalApplicationStatus.FAILED, null, null);
    }

    public static ApplicationStatus failed(String msg) {
        return new ApplicationStatus(FinalApplicationStatus.FAILED, msg, null);
    }

    public static ApplicationStatus failed(String msg, String newTrackUrl) {
        return new ApplicationStatus(FinalApplicationStatus.FAILED, msg, newTrackUrl);
    }

    public static ApplicationStatus succeeded() {
        return new ApplicationStatus(FinalApplicationStatus.SUCCEEDED, null, null);
    }

    public static ApplicationStatus succeeded(String msg) {
        return new ApplicationStatus(FinalApplicationStatus.SUCCEEDED, msg, null);
    }

    public static ApplicationStatus succeeded(String msg, String newTrackUrl) {
        return new ApplicationStatus(FinalApplicationStatus.SUCCEEDED, msg, newTrackUrl);
    }

    private ApplicationStatus(FinalApplicationStatus finalApplicationStatus, String message, String newTrackUrl) {
        this.finalApplicationStatus = finalApplicationStatus;
        this.message = message;
        this.newTrackUrl = newTrackUrl;
    }

    FinalApplicationStatus getFinalApplicationStatus() {
        return finalApplicationStatus;
    }

    String getMessage() {
        return message;
    }

    String getNewTrackUrl() {
        return newTrackUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplicationStatus that = (ApplicationStatus) o;
        return finalApplicationStatus == that.finalApplicationStatus &&
                Objects.equals(message, that.message) &&
                Objects.equals(newTrackUrl, that.newTrackUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finalApplicationStatus, message, newTrackUrl);
    }

    @Override
    public String toString() {
        return "FinalApplicationStatusDetails{" +
                "finalApplicationStatus=" + finalApplicationStatus +
                ", message='" + message + '\'' +
                ", newTrackUrl='" + newTrackUrl + '\'' +
                '}';
    }
}
