package moe.dare.briareus.common.constraint;

import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface Constraint<T> {
    void validate(@Nullable T object) throws ConstraintValidationException;
}
