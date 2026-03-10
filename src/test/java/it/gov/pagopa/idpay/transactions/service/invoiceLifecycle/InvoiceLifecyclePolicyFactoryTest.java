package it.gov.pagopa.idpay.transactions.service.invoiceLifecycle;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InvoiceLifecyclePolicyFactoryTest {

  @Test
  void nullOrEmptyScopesThrows() {
    List<String> emptyScopes = List.of();
    assertThrows(ClientExceptionWithBody.class, () -> InvoiceLifecyclePolicyFactory.fromScopes(null));
    assertThrows(ClientExceptionWithBody.class, () -> InvoiceLifecyclePolicyFactory.fromScopes(emptyScopes));
  }

  @Test
  void selectsFullWhenPresent() {
    InvoiceLifecyclePolicy p = InvoiceLifecyclePolicyFactory.fromScopes(List.of("transaction:reversal:full", "transaction:reversal:basic"));
    assertInstanceOf(FullInvoiceLifecyclePolicy.class, p);
  }

  @Test
  void selectsBasicWhenOnlyBasicPresent() {
    InvoiceLifecyclePolicy p = InvoiceLifecyclePolicyFactory.fromScopes(List.of("transaction:reversal:basic"));
    assertInstanceOf(BasicInvoiceLifecyclePolicy.class, p);
  }

  @Test
  void unsupportedScopesThrows() {
    var scopes = List.of("some:other:scope");
    assertThrows(ClientExceptionWithBody.class, () -> InvoiceLifecyclePolicyFactory.fromScopes(scopes));
  }
}
