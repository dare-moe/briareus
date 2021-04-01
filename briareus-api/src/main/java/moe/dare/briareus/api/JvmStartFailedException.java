package moe.dare.briareus.api;

/**
 * Generic exception showing that app start has failed.
 */
public class JvmStartFailedException extends BriareusException {
    public JvmStartFailedException() {
    }

    public JvmStartFailedException(String message) {
        super(message);
    }

    public JvmStartFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public JvmStartFailedException(Throwable cause) {
        super(cause);
    }
}
