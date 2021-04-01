package moe.dare.briareus.common.utils;

public class Preconditions {
    public static void checkState(boolean condition) {
        if (!condition) {
            throw new IllegalStateException();
        }
    }

    public static void checkState(boolean condition, String msg) {
        if (!condition) {
            throw new IllegalStateException(msg);
        }
    }

    public static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean condition, String msg) {
        if (!condition) {
            throw new IllegalArgumentException(msg);
        }
    }

    private Preconditions() {
    }
}
