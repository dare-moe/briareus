package moe.dare.briareus.common.utils;

import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public final class Either<L, R> {
    private final Object some;
    private final boolean isLeft;

    public static <L, R> Either<L, R> oneOfNullable(L first, R second) {
        if (first == null) {
            if (second == null) {
                throw new IllegalArgumentException("Both first and second are null");
            }
            return new Either<>(second, false);
        } else if (second == null) {
            return new Either<>(first, true);
        } else {
            throw new IllegalArgumentException("Both first and second are not null");
        }
    }

    public static <L, R> Either<L, R> left(L some) {
        requireNonNull(some);
        return new Either<>(some, true);
    }

    public static <L, R> Either<L, R> right(R some) {
        requireNonNull(some);
        return new Either<>(some, false);
    }

    public boolean isLeft() {
        return isLeft;
    }

    public boolean isRight() {
        return !isLeft;
    }

    @SuppressWarnings("unchecked")
    public L left() {
        if (isLeft) {
            return (L) some;
        }
        throw new NoSuchElementException("No left in this either");
    }

    @SuppressWarnings("unchecked")
    public R right() {
        if (!isLeft) {
            return (R) some;
        }
        throw new NoSuchElementException("No right in this either");
    }

    private Either(Object some, boolean isLeft) {
        this.some = some;
        this.isLeft = isLeft;
    }
}
