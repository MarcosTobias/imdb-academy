package co.empathy.academy.search.exception.types;

/**
 * Exception used when the user is trying to remove an index that does not exist
 */
public class IndexDoesNotExistException extends RuntimeException {
    public IndexDoesNotExistException(String s, Exception e) {
        super(s, e);
    }
}
