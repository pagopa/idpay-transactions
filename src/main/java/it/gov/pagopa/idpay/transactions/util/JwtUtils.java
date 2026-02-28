package it.gov.pagopa.idpay.transactions.util;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class JwtUtils {

  private JwtUtils() {}

  /**
   * Decode the JWT token without performing any verification and extract scopes.
   * Throws ResponseStatusException with 403 if header missing or scopes absent.
   */
  public static List<String> extractScopesOrThrow(String authorization) {
    if (authorization == null || authorization.isBlank()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Authorization header missing");
    }

    String token = authorization.trim();
    if (token.toLowerCase().startsWith("bearer ")) {
      token = token.substring(7).trim();
    }
    if (token.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Bearer token missing");
    }

    try {
      DecodedJWT jwt = JWT.decode(token); // no verification performed

      List<String> scopes = new ArrayList<>();
      // TODO check the specification to understand if 'scope' or 'scp' is the standard claim for scopes and if both are allowed
      // First try 'scope' claim
      if (jwt.getClaim("scope") != null && !jwt.getClaim("scope").isNull()) {
        try {
          String[] arr = jwt.getClaim("scope").asArray(String.class);
          if (arr != null) Collections.addAll(scopes, arr);
        } catch (Exception e) {
          String scopeStr = jwt.getClaim("scope").asString();
          if (scopeStr != null && !scopeStr.isBlank()) {
            scopes.addAll(Arrays.asList(scopeStr.split(" ")));
          }
        }
      }

      // Then try 'scp' claim if still empty
      if (scopes.isEmpty() && jwt.getClaim("scp") != null && !jwt.getClaim("scp").isNull()) {
        try {
          String[] arr = jwt.getClaim("scp").asArray(String.class);
          if (arr != null) Collections.addAll(scopes, arr);
        } catch (Exception e) {
          String scpStr = jwt.getClaim("scp").asString();
          if (scpStr != null && !scpStr.isBlank()) {
            scopes.addAll(Arrays.asList(scpStr.split(" ")));
          }
        }
      }

      if (scopes.isEmpty()) {
        throw new ResponseStatusException(HttpStatus.FORBIDDEN, "scope claim missing");
      }

      return scopes;
    } catch (ResponseStatusException ex) {
      throw ex;
    } catch (Exception e) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "invalid token format");
    }
  }
}
