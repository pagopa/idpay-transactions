package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

import java.util.List;

public interface InvoiceLifecyclePolicy {
  /**
   * Return true if the provided scopes match this policy.
   */
  boolean supports(List<String> scopes);

  /**
   * Validate the transaction according to the policy. Returns Mono.empty() when allowed, or Mono.error when not allowed.
   */
  Mono<RewardTransaction> validate(RewardTransaction trx, RewardBatch batch);
}
