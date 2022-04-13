package co.empathy.academy.search.configuration.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

/**
 * Authentication provider for the api key security configuration
 */

@Component
public class ApiKeyAuthProvider implements AuthenticationProvider {
    @Value("${apiKey}")
    private String apiKey1;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String apiKey = (String) authentication.getPrincipal();

        if(ObjectUtils.isEmpty(apiKey)) {
            throw new InsufficientAuthenticationException("The request does not contain an API key");
        } else {
            if(this.apiKey1.equals(apiKey)) {
                return new ApiKeyAuthToken(apiKey, true);
            }

            throw new BadCredentialsException("API key is invalid");
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return ApiKeyAuthToken.class.isAssignableFrom(authentication);
    }
}
