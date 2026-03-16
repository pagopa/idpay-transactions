package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import java.util.List;

import lombok.NonNull;
import org.springframework.http.HttpStatus;

public final class InvoiceLifecyclePolicyFactory {

  private InvoiceLifecyclePolicyFactory() {}

  /**
   * Builds the appropriate policy based on a list of scope values
   * @param scopes list of scope values
   * @return the policy matching the scopes, or throws 403 FORBIDDEN if no valid policy is found
   */
  @NonNull
  public static InvoiceLifecyclePolicy fromScopes(List<String> scopes) {
    if (scopes == null || scopes.isEmpty()) {
      throw new ClientExceptionWithBody(
          HttpStatus.FORBIDDEN, ROLE_NOT_ALLOWED, "No scope present for invoice operations");
    }

    InvoiceLifecyclePolicy full = new FullInvoiceLifecyclePolicy();
    if (full.supports(scopes)) {
      return full;
    }

    InvoiceLifecyclePolicy basic = new BasicInvoiceLifecyclePolicy();
    if (basic.supports(scopes)) {
      return basic;
    }

    throw new ClientExceptionWithBody(
        HttpStatus.FORBIDDEN, ROLE_NOT_ALLOWED, "No allowed scope present for invoice operations");
  }
}
