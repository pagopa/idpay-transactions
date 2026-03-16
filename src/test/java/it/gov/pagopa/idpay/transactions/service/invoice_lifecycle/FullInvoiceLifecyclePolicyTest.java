package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullInvoiceLifecyclePolicyTest {

    private final FullInvoiceLifecyclePolicy policy = new FullInvoiceLifecyclePolicy();

    @Test
    void supportsShouldReturnFalseWhenScopesIsNull() {
        boolean result = policy.supports(null);

        assertFalse(result);
    }

    @Test
    void supportsShouldReturnTrueWhenScopeIsPresent() {
        boolean result = policy.supports(List.of("another:scope", "transaction:invoicelifecycle:full"));

        assertTrue(result);
    }

    @Test
    void supportsShouldReturnFalseWhenScopeIsNotPresent() {
        boolean result = policy.supports(List.of("another:scope", "transaction:invoicelifecycle:basic"));

        assertFalse(result);
    }

    @ParameterizedTest
    @EnumSource(value = SyncTrxStatus.class, names = {"INVOICED", "REWARDED"})
    void validateShouldReturnTransactionWhenAllConditionsAreSatisfied(SyncTrxStatus trxStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(trxStatus.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.CREATED)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void validateShouldAcceptTransactionStatusIgnoringCase() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("rewarded")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.APPROVED)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectNext(trx)
                .verifyComplete();
    }

    @ParameterizedTest
    @EnumSource(value = SyncTrxStatus.class, mode = EnumSource.Mode.EXCLUDE, names = {"INVOICED", "REWARDED"})
    void validateShouldFailWhenTransactionStatusIsNotAllowed(SyncTrxStatus trxStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(trxStatus.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.CREATED)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @ParameterizedTest
    @EnumSource(value = RewardBatchStatus.class, mode = EnumSource.Mode.EXCLUDE, names = {"CREATED", "EVALUATING", "APPROVED", "PENDING_REFUND", "REFUNDED", "NOT_REFUNDED"})
    void validateShouldFailWhenBatchStatusIsNotAllowed(RewardBatchStatus batchStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(batchStatus)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @ParameterizedTest
    @EnumSource(
            value = RewardBatchTrxStatus.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"CONSULTABLE", "TO_CHECK", "SUSPENDED", "REJECTED"}
    )
    void validateShouldFailWhenRewardBatchTrxStatusIsNotAllowed(RewardBatchTrxStatus batchTrxStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.REWARDED.name())
                .rewardBatchTrxStatus(batchTrxStatus)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.EVALUATING)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @ParameterizedTest
    @EnumSource(value = RewardBatchTrxStatus.class, names = {"CONSULTABLE", "TO_CHECK", "SUSPENDED", "REJECTED"})
    void validateShouldReturnTransactionForAllAllowedRewardBatchTrxStatuses(RewardBatchTrxStatus batchTrxStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchTrxStatus(batchTrxStatus)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.APPROVED)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectNext(trx)
                .verifyComplete();
    }
}