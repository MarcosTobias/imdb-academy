package co.empathy.academy.search.exception;

import co.empathy.academy.search.exception.types.IndexAlreadyExistsException;
import co.empathy.academy.search.exception.types.IndexDoesNotExistException;
import co.empathy.academy.search.exception.types.InternalServerException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Middleware for catching business exceptions and returning information regarding the error
 */
@ControllerAdvice
public class RestExceptionHandler extends ResponseEntityExceptionHandler {
    /**
     * Handler for bad request exceptions
     * @param e exception that was thrown
     * @param request in which the exception occurred
     * @return JSON with the response and the error code
     */
    @ExceptionHandler(value = { IndexAlreadyExistsException.class, IndexDoesNotExistException.class })
    protected ResponseEntity<Object> handleConflictBadRequest(RuntimeException e, WebRequest request) {
        return buildResponseEntity(new ApiError(HttpStatus.BAD_REQUEST, e.getMessage(), e.getCause()));
    }

    /**
     * Creates the response entity based on an apiError object
     * @param apiError containing the details about the error
     * @return ResponseEntity with the response for the user
     */
    private ResponseEntity<Object> buildResponseEntity(ApiError apiError) {
        return new ResponseEntity<>(apiError, apiError.getStatus());
    }

    /**
     * Handler for server error exceptions
     * @param e exception that was thrown
     * @param request in which the exception occurred
     * @return JSON with the response and the error code
     */
    @ExceptionHandler(value = { InternalServerException.class })
    protected ResponseEntity<Object> handleConflictInternal(RuntimeException e, WebRequest request) {
        return buildResponseEntity(new ApiError(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getCause()));
    }
    

}
