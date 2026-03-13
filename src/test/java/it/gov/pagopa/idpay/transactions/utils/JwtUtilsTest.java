package it.gov.pagopa.idpay.transactions.utils;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JwtUtilsTest {

  // A standard dummy JWT header ({"alg":"none"})
  private static final String DUMMY_HEADER = "eyJhbGciOiJub25lIn0";
  private static final String DUMMY_SIGNATURE = "dummy_signature";

  /**
   * Helper method to generate a fake JWT with a specific JSON payload.
   */
  private String createMockJwt(String jsonPayload) {
    String base64Payload = Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(jsonPayload.getBytes(StandardCharsets.UTF_8));
    return DUMMY_HEADER + "." + base64Payload + "." + DUMMY_SIGNATURE;
  }

  // --- Happy Path Tests (Token Parsing) ---

  @Test
  void extractScopesOrThrow_withScopeClaimAsString_returnsList() {
    String payload = "{\"scope\": \"transaction.read transaction.write\"}";
    String token = "Bearer " + createMockJwt(payload);

    List<String> scopes = JwtUtils.extractScopesOrThrow(token);

    assertEquals(2, scopes.size());
    assertTrue(scopes.contains("transaction.read"));
    assertTrue(scopes.contains("transaction.write"));
  }

  @Test
  void extractScopesOrThrow_withScopeClaimAsArray_returnsList() {
    String payload = "{\"scope\": [\"transaction.read\", \"transaction.write\"]}";
    String token = "Bearer " + createMockJwt(payload);

    List<String> scopes = JwtUtils.extractScopesOrThrow(token);

    assertEquals(2, scopes.size());
    assertTrue(scopes.contains("transaction.read"));
    assertTrue(scopes.contains("transaction.write"));
  }

  // --- Error Handling Tests ---
  @Test
  void extractScopesOrThrow_nullOrBlankHeader_throwsForbidden() {
    ResponseStatusException ex1 = assertThrows(ResponseStatusException.class,
        () -> JwtUtils.extractScopesOrThrow(null));
    assertEquals(HttpStatus.FORBIDDEN, ex1.getStatusCode());
    assertEquals("Authorization header missing", ex1.getReason());

    ResponseStatusException ex2 = assertThrows(ResponseStatusException.class,
        () -> JwtUtils.extractScopesOrThrow("   "));
    assertEquals(HttpStatus.FORBIDDEN, ex2.getStatusCode());
  }

  @Test
  void extractScopesOrThrow_emptyBearerToken_throwsForbidden() {
    ResponseStatusException ex = assertThrows(ResponseStatusException.class,
        () -> JwtUtils.extractScopesOrThrow("Bearer "));

    assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
    assertEquals("Bearer token missing", ex.getReason());
  }

  @Test
  void extractScopesOrThrow_invalidJwtStructure_throwsForbidden() {
    // Token without dots
    ResponseStatusException ex = assertThrows(ResponseStatusException.class,
        () -> JwtUtils.extractScopesOrThrow("Bearer notarealjwt"));

    assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
    assertEquals("Invalid JWT structure", ex.getReason());
  }

  @Test
  void extractScopesOrThrow_missingScopeClaim_throwsForbidden() {
    String payload = "{\"sub\": \"user123\", \"email\": \"test@test.com\"}";
    String token = "Bearer " + createMockJwt(payload);

    ResponseStatusException ex = assertThrows(ResponseStatusException.class,
        () -> JwtUtils.extractScopesOrThrow(token));

    assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
    assertEquals("Scope claim missing", ex.getReason());
  }

  @Test
  void extractScopesOrThrow_invalidBase64Payload_throwsForbidden() {
    String token = "Bearer header.!@#invalid_base64_payload$$$.signature";

    ResponseStatusException ex = assertThrows(ResponseStatusException.class,
        () -> JwtUtils.extractScopesOrThrow(token));

    assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
    assertEquals("Invalid token format", ex.getReason());
  }
}