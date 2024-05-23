package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ConsumerTimeoutException
 * @description:
 * @datetime 2024年 05月 22日 17:42
 * @version: 1.0
 */
/**
 * ConsumerTimeoutException is thrown when a timeout occurs while consuming messages.
 */
public class ConsumerTimeoutException extends RuntimeException {

    /**
     * Constructs a new ConsumerTimeoutException with no detail message.
     */
    public ConsumerTimeoutException() {
        super();
    }

    /**
     * Constructs a new ConsumerTimeoutException with the specified detail message.
     * @param message the detail message.
     */
    public ConsumerTimeoutException(String message) {
        super(message);
    }

    /**
     * Constructs a new ConsumerTimeoutException with the specified cause.
     * @param cause the cause (which is saved for later retrieval by the Throwable.getCause() method).
     */
    public ConsumerTimeoutException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new ConsumerTimeoutException with the specified detail message and cause.
     * @param message the detail message.
     * @param cause the cause (which is saved for later retrieval by the Throwable.getCause() method).
     */
    public ConsumerTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}