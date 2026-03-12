package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.springframework.http.HttpStatus;

import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED;

public final class InvoiceLifecyclePolicyFactory {

  private InvoiceLifecyclePolicyFactory() {}

  public static InvoiceLifecyclePolicy fromScopes(List<String> scopes) {
    if (scopes == null || scopes.isEmpty()) {
      throw new ClientExceptionWithBody(HttpStatus.FORBIDDEN, ROLE_NOT_ALLOWED, "No scope present for invoice operations");
    }

    InvoiceLifecyclePolicy full = new FullInvoiceLifecyclePolicy();
    if (full.supports(scopes)) return full;

    InvoiceLifecyclePolicy basic = new BasicInvoiceLifecyclePolicy();
    if (basic.supports(scopes)) return basic;

    throw new ClientExceptionWithBody(HttpStatus.FORBIDDEN, ROLE_NOT_ALLOWED, "No allowed scope present for invoice operations");
  }
}
