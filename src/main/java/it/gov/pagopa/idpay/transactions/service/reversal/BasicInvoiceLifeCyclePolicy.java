package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_STATUS_NOT_ALLOWED;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.TRANSACTION_STATUS_NOT_ALLOWED;

public class BasicInvoiceLifeCyclePolicy implements InvoiceLifeCyclePolicy {

  private static final String SCOPE = "transaction:reversal:basic";

  @Override
  public boolean supports(List<String> scopes) {
    return scopes != null && scopes.stream().anyMatch(SCOPE::equals);
  }

  @Override
  public Mono<RewardTransaction> validate(RewardTransaction trx, RewardBatch batch) {
    String status = trx.getStatus();
    RewardBatchStatus batchStatus = batch.getStatus();

      if (!SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status)) {
          return Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, TRANSACTION_STATUS_NOT_ALLOWED,
                  "Transaction status not allowed for full reversal"));
      }

      boolean batchStatusAllowed = RewardBatchStatus.CREATED.equals(batchStatus)
              || RewardBatchStatus.EVALUATING.equals(batchStatus)
              || RewardBatchStatus.APPROVED.equals(batchStatus);

      if (!batchStatusAllowed) {
          return Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_STATUS_NOT_ALLOWED,
                  "Batch status not allowed for full reversal"));
      }

      return Mono.just(trx);
  }
}
