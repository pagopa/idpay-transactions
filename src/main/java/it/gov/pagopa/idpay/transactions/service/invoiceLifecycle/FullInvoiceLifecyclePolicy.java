package it.gov.pagopa.idpay.transactions.service.invoiceLifecycle;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;

public class FullInvoiceLifecyclePolicy implements InvoiceLifecyclePolicy {

  private static final String SCOPE = "transaction:invoicelifecycle:full";

  @Override
  public boolean supports(List<String> scopes) {
    return scopes != null && scopes.stream().anyMatch(SCOPE::equals);
  }

  @Override
  public Mono<RewardTransaction> validate(RewardTransaction trx, RewardBatch batch) {
    String status = trx.getStatus();
    RewardBatchTrxStatus batchTrxStatus = trx.getRewardBatchTrxStatus();
    RewardBatchStatus batchStatus = batch.getStatus();

    boolean trxStatusAllowed = SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status)
        || SyncTrxStatus.REWARDED.name().equalsIgnoreCase(status);

      if (!trxStatusAllowed) {
          return Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, TRANSACTION_STATUS_NOT_ALLOWED,
                  "Transaction status not allowed for full invoice operations"));
      }

    boolean batchStatusAllowed = RewardBatchStatus.CREATED.equals(batchStatus)
            || RewardBatchStatus.EVALUATING.equals(batchStatus)
            || RewardBatchStatus.APPROVED.equals(batchStatus);

      if (!batchStatusAllowed) {
          return Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_STATUS_NOT_ALLOWED,
                  "Batch status not allowed for full invoice operations"));
      }

    boolean trxBatchStatusAllowed = RewardBatchTrxStatus.CONSULTABLE.equals(batchTrxStatus)
            || RewardBatchTrxStatus.TO_CHECK.equals(batchTrxStatus)
            || RewardBatchTrxStatus.SUSPENDED.equals(batchTrxStatus)
            || RewardBatchTrxStatus.REJECTED.equals(batchTrxStatus);

      if (!trxBatchStatusAllowed) {
          return Mono.error(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_TRX_STATUS_NOT_ALLOWED,
                  "RewardBatchTrxStatus not allowed for full invoice operations"));
      }

        return Mono.just(trx);

  }
}
