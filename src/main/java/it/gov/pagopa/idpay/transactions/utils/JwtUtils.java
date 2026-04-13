package it.gov.pagopa.idpay.transactions.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class JwtUtils {

  // Thread-safe, instantiate once to save memory/CPU
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JwtUtils() {}

  /**
   * Decodes the JWT payload to extract scopes without verifying the signature. Throws
   * ResponseStatusException with 403 if header is missing or scopes are absent.
   */
  public static List<String> extractScopesOrThrow(String authorization) {
      JsonNode root = extractPayloadOrThrow(authorization);

      List<String> scopes = extractClaimAsList(root, "scope");

      if (scopes.isEmpty()) {
          throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Scope claim missing");
      }

      return scopes;
  }

  public static String extractOrganizationIdOrThrow(String authorization) {
      JsonNode root = extractPayloadOrThrow(authorization);

      String orgId = extractClaimAsText(root, "org_id");

      if (orgId == null || orgId.isBlank()) {
          throw new ResponseStatusException(HttpStatus.FORBIDDEN, "org_id claim missing");
      }

      return orgId;
  }

  private static JsonNode extractPayloadOrThrow(String authorization) {
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
          String[] chunks = token.split("\\.");
          if (chunks.length < 2) {
              throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid JWT structure");
          }

          byte[] decodedPayload = Base64.getUrlDecoder().decode(chunks[1]);
          return MAPPER.readTree(new String(decodedPayload, StandardCharsets.UTF_8));

      } catch (ResponseStatusException ex) {
          throw ex;
      } catch (Exception e) {
          throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid token format");
      }
  }

  private static String extractClaimAsText(JsonNode root, String claimName) {
      JsonNode node = root.path(claimName);

      if (node.isMissingNode() || node.isNull()) {
          return null;
      }

      if (node.isTextual()) {
          String text = node.asText().trim();
          return text.isEmpty() ? null : text;
      }

      return node.asText(null);
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
