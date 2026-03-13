package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

import java.util.List;

public interface InvoiceLifecyclePolicy {

  /**
   * Check if the current policy supports one of the provided scopes
   * @param scopes list of scopes to check
   * @return true if the policy supports at least one of the provided scopes, false otherwise
   */
  boolean supports(List<String> scopes);

  /**
   * Validates the transaction/batch couple against current policy.
   * @param trx Transaction to validate
   * @param batch Batch to validate
   * @return a Mono emitting the transaction if validation is successful, or an error if validation fails
   */
  Mono<RewardTransaction> validate(RewardTransaction trx, RewardBatch batch);
}
