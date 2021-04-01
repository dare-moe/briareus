package moe.dare.briareus.api;

/**
 * Interface for option keys. Also works as <i>type token</i>.
 *
 * @param <T> type of option value.
 * @see RemoteJvmOptions
 */
public interface OptKey<T> {
    /**
     * @param value validates that provided value is valid for this option.
     */
    void validate(T value);

    /**
     * @param object an object
     * @return casted object
     */
    T cast(Object object);
}
