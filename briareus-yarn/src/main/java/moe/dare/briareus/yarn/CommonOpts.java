package moe.dare.briareus.yarn;

import moe.dare.briareus.api.OptKey;
import moe.dare.briareus.common.constraint.Constraint;
import moe.dare.briareus.common.constraint.ConstraintValidationException;
import moe.dare.briareus.common.constraint.Constraints;

public class CommonOpts {
    /**
     * Container cores for application
     */
    public static final OptKey<Integer> YARN_CONTAINER_CORES = IntOpts.YARN_CONTAINER_CORES;
    public static final OptKey<Long> YARN_CONTAINER_MEMORY_MB = LongOpts.YARN_CONTAINER_MEMORY_MB;

    private CommonOpts() {
    }

    private enum IntOpts implements OptKey<Integer> {
        YARN_CONTAINER_CORES(Constraints.greaterOrEqual(1));

        private final Constraint<Integer> constraint;

        IntOpts(Constraint<Integer> constraint) {
            this.constraint = constraint;
        }

        @Override
        public void validate(Integer value) {
            try {
                constraint.validate(value);
            } catch (ConstraintValidationException e) {
                throw new IllegalArgumentException("Bad value [" + value + "] for " + toString(), e);
            }
        }

        @Override
        public Integer cast(Object object) {
            return (Integer) object;
        }
    }

    private enum LongOpts implements OptKey<Long> {
        YARN_CONTAINER_MEMORY_MB(Constraints.greaterOrEqual(1L));

        private final Constraint<Long> constraint;

        LongOpts(Constraint<Long> constraint) {
            this.constraint = constraint;
        }

        @Override
        public void validate(Long value) {
            try {
                constraint.validate(value);
            } catch (ConstraintValidationException e) {
                throw new IllegalArgumentException("Bad value [" + value + "] for " + toString(), e);
            }
        }

        @Override
        public Long cast(Object object) {
            return (Long) object;
        }
    }
}
