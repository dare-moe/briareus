package moe.dare.briareus.common.constraint;

import java.util.Comparator;

import static java.util.Objects.requireNonNull;

public class Constraints {
    @SafeVarargs
    public static <T> Constraint<T> allOf(Constraint<? super T>... constraints) {
        requireNonNull(constraints);
        for (Constraint<? super T> constraint : constraints) {
            requireNonNull(constraint);
        }
        return object -> {
            validateNotNull(object);
            for (Constraint<? super T> constraint : constraints) {
                constraint.validate(object);
            }
        };
    }

    public static <T> Constraint<T> greater(T value, Comparator<? super T> comparator) {
        requireNonNull(value);
        requireNonNull(comparator);
        return object -> {
            validateNotNull(object);
            if (comparator.compare(object, value) <= 0) {
                throw new ConstraintValidationException("Value not greater than " + value);
            }
        };
    }

    public static <T extends Comparable<? super T>> Constraint<T> greater(T value) {
        return greater(value, Comparator.naturalOrder());
    }

    public static <T> Constraint<T> greaterOrEqual(T value, Comparator<? super T> comparator) {
        requireNonNull(value);
        requireNonNull(comparator);
        return object -> {
            validateNotNull(object);
            if (comparator.compare(object, value) < 0) {
                throw new ConstraintValidationException("Value not greater or equal than " + value);
            }
        };
    }

    public static <T extends Comparable<? super T>> Constraint<T> greaterOrEqual(T value) {
        return greaterOrEqual(value, Comparator.naturalOrder());
    }

    public static <T> Constraint<T> less(T value, Comparator<? super T> comparator) {
        requireNonNull(value);
        requireNonNull(comparator);
        return object -> {
            validateNotNull(object);
            if (comparator.compare(object, value) >= 0) {
                throw new ConstraintValidationException("Value not less than " + value);
            }
        };
    }

    public static <T extends Comparable<? super T>> Constraint<T> less(T value) {
        return less(value, Comparator.naturalOrder());
    }

    public static <T> Constraint<T> lessOrEqual(T value, Comparator<? super T> comparator) {
        requireNonNull(value);
        requireNonNull(comparator);
        return object -> {
            validateNotNull(object);
            if (comparator.compare(object, value) > 0) {
                throw new ConstraintValidationException("Value not less or equal than " + value);
            }
        };
    }

    public static <T extends Comparable<? super T>> Constraint<T> lessOrEqual(T value) {
        return lessOrEqual(value, Comparator.naturalOrder());
    }

    public static Constraint<String> notEmptyString() {
        return string -> {
            validateNotNull(string);
            if (string.isEmpty()) {
                throw new ConstraintValidationException("Empty strings are not permitted");
            }
        };
    }

    public static <T> Constraint<T> notNull() {
        return Constraints::validateNotNull;
    }

    private static void validateNotNull(Object object) throws ConstraintValidationException {
        if (object == null) {
            throw new ConstraintValidationException("Null values are not permitted");
        }
    }

    private Constraints() {
    }
}
