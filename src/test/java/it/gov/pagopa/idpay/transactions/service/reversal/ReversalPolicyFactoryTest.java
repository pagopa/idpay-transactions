package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReversalPolicyFactoryTest {

  @Test
  void nullOrEmptyScopesThrows() {
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(null));
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(List.of()));
  }

  @Test
  void selectsFullWhenPresent() {
    ReversalPolicy p = ReversalPolicyFactory.fromScopes(List.of("api:storno:basic", "api:storno:full"));
    assertTrue(p instanceof FullReversalPolicy);
  }

  @Test
  void selectsBasicWhenOnlyBasicPresent() {
    ReversalPolicy p = ReversalPolicyFactory.fromScopes(List.of("api:storno:basic"));
    assertTrue(p instanceof BasicReversalPolicy);
  }

  @Test
  void unsupportedScopesThrows() {
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(List.of("some:other:scope")));
  }
}
