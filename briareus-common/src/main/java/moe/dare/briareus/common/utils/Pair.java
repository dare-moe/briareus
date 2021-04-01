package moe.dare.briareus.common.utils;

import static java.util.Objects.requireNonNull;

public final class Pair<T, R> {
    private final T first;
    private final R second;

    public static <L, R> Pair<L, R> of(L first, R second) {
        return new Pair<>(first, second);
    }

    public T first() {
        return first;
    }

    public R second() {
        return second;
    }

    private Pair(T first, R second) {
        this.first = requireNonNull(first, "first");
        this.second = requireNonNull(second, "second");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        if (!first.equals(pair.first)) return false;
        return second.equals(pair.second);
    }

    @Override
    public int hashCode() {
        return 31 * first.hashCode() + second.hashCode();
    }
}
