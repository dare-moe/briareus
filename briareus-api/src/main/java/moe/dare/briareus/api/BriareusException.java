package moe.dare.briareus.api;

/**
 * The base class of all briareus exceptions.
 */
public class BriareusException extends RuntimeException {
    public BriareusException() {
        super();
    }

    public BriareusException(String message) {
        super(message);
    }

    public BriareusException(String message, Throwable cause) {
        super(message, cause);
    }

    public BriareusException(Throwable cause) {
        super(cause);
    }
}
