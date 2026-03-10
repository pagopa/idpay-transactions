package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InvoiceLifeCyclePolicyFactoryTest {

  @Test
  void nullOrEmptyScopesThrows() {
    List<String> emptyScopes = List.of();
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(null));
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(emptyScopes));
  }

  @Test
  void selectsFullWhenPresent() {
    InvoiceLifeCyclePolicy p = ReversalPolicyFactory.fromScopes(List.of("transaction:reversal:full", "transaction:reversal:basic"));
    assertInstanceOf(FullInvoiceLifeCyclePolicy.class, p);
  }

  @Test
  void selectsBasicWhenOnlyBasicPresent() {
    InvoiceLifeCyclePolicy p = ReversalPolicyFactory.fromScopes(List.of("transaction:reversal:basic"));
    assertInstanceOf(BasicInvoiceLifeCyclePolicy.class, p);
  }

  @Test
  void unsupportedScopesThrows() {
    var scopes = List.of("some:other:scope");
    assertThrows(ClientExceptionWithBody.class, () -> ReversalPolicyFactory.fromScopes(scopes));
  }
}
