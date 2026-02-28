package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReversalPolicyFactoryTest {

  @Test
  void nullOrEmptyScopesThrows() {
    List<String> emptyScopes = List.of();
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(null));
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(emptyScopes));
  }

  @Test
  void selectsFullWhenPresent() {
    ReversalPolicy p = ReversalPolicyFactory.fromScopes(List.of("api:storno:basic", "api:storno:full"));
    assertInstanceOf(FullReversalPolicy.class, p);
  }

  @Test
  void selectsBasicWhenOnlyBasicPresent() {
    ReversalPolicy p = ReversalPolicyFactory.fromScopes(List.of("api:storno:basic"));
    assertInstanceOf(BasicReversalPolicy.class, p);
  }

  @Test
  void unsupportedScopesThrows() {
    var scopes = List.of("some:other:scope");
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(scopes));
  }
}
