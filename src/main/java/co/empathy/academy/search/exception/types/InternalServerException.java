package co.empathy.academy.search.exception.types;

/**
 * Exception used when a server error occurs on a request
 */
public class InternalServerException extends RuntimeException {
    public InternalServerException(String message, Exception e) {
        super(message, e);
    }
}
