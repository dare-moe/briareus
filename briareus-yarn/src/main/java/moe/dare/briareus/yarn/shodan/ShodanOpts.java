package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.api.OptKey;
import moe.dare.briareus.common.constraint.Constraint;
import moe.dare.briareus.common.constraint.ConstraintValidationException;
import moe.dare.briareus.common.constraint.Constraints;

public class ShodanOpts {
    public static final OptKey<String> YARN_QUEUE = StringOpts.YARN_QUEUE;
    public static final OptKey<String> YARN_APPLICATION_NAME = StringOpts.YARN_APPLICATION_NAME;
    public static final OptKey<Boolean> KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS = BooleanOpts.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS;
    public static final OptKey<Integer> YARN_APPLICATION_ATTEMPTS = IntOpts.YARN_APPLICATION_ATTEMPTS;
    public static final OptKey<Integer> YARN_APPLICATION_PRIORITY = IntOpts.YARN_APPLICATION_PRIORITY;

    private enum StringOpts implements OptKey<String> {
        YARN_QUEUE(Constraints.notEmptyString()),
        YARN_APPLICATION_NAME(Constraints.notEmptyString());

        private final Constraint<String> constraint;

        StringOpts(Constraint<String> constraint) {
            this.constraint = constraint;
        }


        @Override
        public void validate(String value) {
            try {
                constraint.validate(value);
            } catch (ConstraintValidationException e) {
                throw new IllegalArgumentException("Bad value [" + value + "] for " + toString(), e);
            }
        }

        @Override
        public String cast(Object object) {
            return (String) object;
        }
    }

    private enum IntOpts implements OptKey<Integer> {
        YARN_APPLICATION_ATTEMPTS(Constraints.greaterOrEqual(1)),
        YARN_APPLICATION_PRIORITY(Constraints.notNull());

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

    private enum BooleanOpts implements OptKey<Boolean> {
        KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS(Constraints.notNull());

        private final Constraint<Boolean> constraint;

        BooleanOpts(Constraint<Boolean> constraint) {
            this.constraint = constraint;
        }


        @Override
        public void validate(Boolean value) {
            try {
                constraint.validate(value);
            } catch (ConstraintValidationException e) {
                throw new IllegalArgumentException("Bad value [" + value + "] for " + toString(), e);
            }
        }

        @Override
        public Boolean cast(Object object) {
            return (Boolean) object;
        }
    }

    private ShodanOpts() {
    }
}
