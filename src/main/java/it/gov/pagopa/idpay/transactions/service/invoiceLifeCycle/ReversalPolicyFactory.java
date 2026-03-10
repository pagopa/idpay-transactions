package it.gov.pagopa.idpay.transactions.service.invoiceLifeCycle;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.springframework.http.HttpStatus;

import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED;

public final class ReversalPolicyFactory {

  private ReversalPolicyFactory() {}

  public static InvoiceLifeCyclePolicy fromScopes(List<String> scopes) {
    if (scopes == null || scopes.isEmpty()) {
      throw new ClientExceptionWithBody(HttpStatus.FORBIDDEN, ROLE_NOT_ALLOWED, "No scope present for invoice operations");
    }

    InvoiceLifeCyclePolicy full = new FullInvoiceLifeCyclePolicy();
    if (full.supports(scopes)) return full;

    InvoiceLifeCyclePolicy basic = new BasicInvoiceLifeCyclePolicy();
    if (basic.supports(scopes)) return basic;

    throw new ClientExceptionWithBody(HttpStatus.FORBIDDEN, ROLE_NOT_ALLOWED, "No allowed scope present for invoice operations");
  }
}
