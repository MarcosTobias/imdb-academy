package co.empathy.academy.search.exception;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

/**
 * JSON representation of an error
 */
@Getter
@Setter
public class ApiError {
    //HTTP status that is going to be returned
    private HttpStatus status;

    //Date and time of the error
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss")
    private LocalDateTime timestamp;

    //Custom message of the error
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private String message;

    //Trace of the error
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private String debugMessage;

    private ApiError() {
        timestamp = LocalDateTime.now();
    }

    ApiError(HttpStatus status) {
        this();
        this.status = status;
    }

    ApiError(HttpStatus status, Throwable ex) {
        this(status);
        this.message = "Unexpected error";
        this.debugMessage = ex.getLocalizedMessage();
    }

    ApiError(HttpStatus status, String message, Throwable ex) {
        this(status, ex);
        this.message = message;
    }
}
