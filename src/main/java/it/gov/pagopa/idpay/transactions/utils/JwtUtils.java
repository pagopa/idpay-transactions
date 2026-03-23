package it.gov.pagopa.idpay.transactions.utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public final class JwtUtils {

  // Thread-safe, instantiate once to save memory/CPU
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JwtUtils() {}

  /**
   * Decodes the JWT payload to extract scopes without verifying the signature. Throws
   * ResponseStatusException with 403 if header is missing or scopes are absent.
   */
  public static List<String> extractScopesOrThrow(String authorization) {
    if (authorization == null || authorization.isBlank()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Authorization header missing");
    }

    final String token =
        authorization.toLowerCase().startsWith("bearer ")
            ? authorization.substring(7).trim()
            : authorization.trim();

    if (token.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Bearer token missing");
    }

    try {
      // JWT format is header.payload.signature
      String[] chunks = token.split("\\.");
      if (chunks.length < 2) {
        throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid JWT structure");
      }

      // Decode only the payload (the second chunk)
      byte[] decodedPayload = Base64.getUrlDecoder().decode(chunks[1]);
      JsonNode root = MAPPER.readTree(new String(decodedPayload, StandardCharsets.UTF_8));

      // Extract claim 'scope' (RFC standard)
      List<String> scopes = extractClaimAsList(root, "scope");

      if (scopes.isEmpty()) {
        throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Scope claim missing");
      }

      return scopes;

    } catch (ResponseStatusException ex) {
      throw ex; // Rethrow HTTP exceptions so they don't get swallowed
    } catch (Exception e) {
      // Catch Base64 or Jackson parsing errors
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid token format");
    }
  }

  /** Helper method to parse a claim that might be a String or a JSON Array. */
  private static List<String> extractClaimAsList(JsonNode root, String claimName) {
    JsonNode node = root.path(claimName);

    if (node.isMissingNode() || node.isNull()) {
      return List.of();
    }

    if (node.isArray()) {
      List<String> result = new ArrayList<>();
      node.forEach(n -> result.add(n.asText()));
      return result;
    }

    if (node.isTextual()) {
      String text = node.asText().trim();
      return text.isEmpty() ? List.of() : List.of(text.split("\\s+"));
    }

    return List.of();
  }
}
