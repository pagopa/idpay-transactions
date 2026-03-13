package it.gov.pagopa.idpay.transactions.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public final class JwtUtils {

  // Thread-safe, instantiate once to save memory/CPU
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JwtUtils() {}

  /**
   * Decodes the JWT payload to extract scopes without verifying the signature.
   * Throws ResponseStatusException with 403 if header is missing or scopes are absent.
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

  /**
   * Helper method to parse a claim that might be a String or a JSON Array.
   */
  private static List<String> extractClaimAsList(JsonNode root, String claimName) {
    List<String> result = new ArrayList<>();

    JsonNode node = root.get(claimName);
    if (node == null || node.isNull()) {
      return result;
    }

    if (node.isArray()) {
      node.forEach(n -> result.add(n.asText()));
      return result;
    }

    if (!node.isTextual()) {
      return result;
    }

    String text = node.asText();
    if (text.isBlank()) {
      return result;
    }

    for (String scope : text.trim().split("\\s+")) {
      result.add(scope);
    }

    return result;
  }
}