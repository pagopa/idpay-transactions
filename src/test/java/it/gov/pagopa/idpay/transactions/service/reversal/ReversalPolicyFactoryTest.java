package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReversalPolicyFactoryTest {

  @Test
  void selectsFullPolicyWhenScopePresent() {
    ReversalPolicy p = ReversalPolicyFactory.fromScopes(List.of("api:storno:full"));
    assertTrue(p instanceof FullReversalPolicy);
  }

  @Test
  void selectsBasicPolicyWhenScopePresent() {
    ReversalPolicy p = ReversalPolicyFactory.fromScopes(List.of("api:storno:basic"));
    assertTrue(p instanceof BasicReversalPolicy);
  }

  @Test
  void forbiddenWhenNoScopes() {
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(List.of()));
  }
}
