package it.gov.pagopa.idpay.transactions.service.reversal;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.TRANSACTION_STATUS_NOT_ALLOWED;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.http.HttpStatus;
import reactor.test.StepVerifier;

class FullReversalPolicyTest {

  private final FullReversalPolicy policy = new FullReversalPolicy();

  // Happy Path
  @ParameterizedTest(name = "Should allow reversal for valid TrxStatus={0} and BatchStatus={1}")
  @CsvSource({
      "INVOICED, TO_CHECK",
      "INVOICED, CONSULTABLE",
      "INVOICED, SUSPENDED",
      "REWARDED, TO_CHECK",
      "REWARDED, CONSULTABLE",
      "REWARDED, SUSPENDED",
      "REWARDED, REJECTED"
  })
  void validate_whenStatusIsValid_shouldAllow(SyncTrxStatus trxStatus,
      RewardBatchTrxStatus batchStatus) {
    RewardTransaction rt = createTransaction(trxStatus, batchStatus);
    StepVerifier.create(policy.validate(rt)).verifyComplete();
  }

  // Unhappy path
  @ParameterizedTest(name = "Should block reversal when TrxStatus is invalid ({0})")
  @EnumSource(value = SyncTrxStatus.class, names = {"INVOICED",
      "REWARDED"}, mode = EnumSource.Mode.EXCLUDE)
  void validate_whenTrxStatusIsInvalid_shouldBlock(SyncTrxStatus invalidTrxStatus) {
    RewardTransaction rt = createTransaction(invalidTrxStatus, RewardBatchTrxStatus.TO_CHECK);

    StepVerifier.create(policy.validate(rt))
        .expectErrorMatches(this::isExpectedException)
        .verify();
  }

  // 3. Testa la regola: Se il BatchStatus è APPROVED, fallisce sempre
  @ParameterizedTest(name = "Should block reversal when BatchStatus is APPROVED, even if TrxStatus is {0}")
  @EnumSource(value = SyncTrxStatus.class, names = {"INVOICED", "REWARDED"})
  void validate_whenBatchStatusIsApproved_shouldBlock(SyncTrxStatus validTrxStatus) {
    RewardTransaction rt = createTransaction(validTrxStatus, RewardBatchTrxStatus.APPROVED);

    StepVerifier.create(policy.validate(rt))
        .expectErrorMatches(this::isExpectedException)
        .verify();
  }

  // Metodi di utilità per mantenere i test puliti
  private RewardTransaction createTransaction(SyncTrxStatus trxStatus,
      RewardBatchTrxStatus batchStatus) {
    RewardTransaction rt = new RewardTransaction();
    rt.setStatus(trxStatus.name());
    rt.setRewardBatchTrxStatus(batchStatus);
    return rt;
  }

  private boolean isExpectedException(Throwable ex) {
    return ex instanceof ClientExceptionWithBody ce &&
        ce.getHttpStatus() == HttpStatus.UNPROCESSABLE_ENTITY &&
        TRANSACTION_STATUS_NOT_ALLOWED.equals(ce.getCode());
  }
}