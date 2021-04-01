package moe.dare.briareus.common.constraint;

public class ConstraintValidationException extends Exception {
    public ConstraintValidationException() {
    }

    public ConstraintValidationException(String message) {
        super(message);
    }

    public ConstraintValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConstraintValidationException(Throwable cause) {
        super(cause);
    }
}
