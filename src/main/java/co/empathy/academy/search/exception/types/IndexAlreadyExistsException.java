package co.empathy.academy.search.exception.types;

/**
 * Exception used when the user is trying to create an index that already exists
 */
public class IndexAlreadyExistsException extends RuntimeException {
    public IndexAlreadyExistsException(String message, Exception e) {
        super(message, e);
    }
}
