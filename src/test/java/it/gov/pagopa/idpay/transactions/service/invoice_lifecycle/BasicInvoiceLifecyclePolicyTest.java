package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
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

class BasicInvoiceLifecyclePolicyTest {

    private final BasicInvoiceLifecyclePolicy policy = new BasicInvoiceLifecyclePolicy();

    @Test
    void supportsShouldReturnFalseWhenScopesIsNull() {
        boolean result = policy.supports(null);

        assertFalse(result);
    }

    @Test
    void supportsShouldReturnTrueWhenScopeIsPresent() {
        boolean result = policy.supports(List.of("another:scope", "transaction:invoicelifecycle:basic"));

        assertTrue(result);
    }

    @Test
    void supportsShouldReturnFalseWhenScopeIsNotPresent() {
        boolean result = policy.supports(List.of("another:scope", "transaction:invoicelifecycle:advanced"));

        assertFalse(result);
    }

    @ParameterizedTest
    @EnumSource(value = RewardBatchStatus.class, names = {"CREATED", "EVALUATING", "APPROVED"})
    void validateShouldReturnTransactionWhenStatusIsInvoicedAndBatchStatusIsAllowed(RewardBatchStatus batchStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(batchStatus)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectNext(trx)
                .verifyComplete();
    }

    @ParameterizedTest
    @EnumSource(value = RewardBatchStatus.class, mode = EnumSource.Mode.EXCLUDE, names = {"CREATED", "EVALUATING", "APPROVED"})
    void validateShouldFailWhenStatusIsInvoicedButBatchStatusIsNotAllowed(RewardBatchStatus batchStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(SyncTrxStatus.INVOICED.name())
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(batchStatus)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @ParameterizedTest
    @EnumSource(value = SyncTrxStatus.class, mode = EnumSource.Mode.EXCLUDE, names = {"INVOICED"})
    void validateShouldFailWhenTransactionStatusIsNotInvoiced(SyncTrxStatus trxStatus) {
        RewardTransaction trx = RewardTransaction.builder()
                .status(trxStatus.name())
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.CREATED)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectError(ClientExceptionWithBody.class)
                .verify();
    }

    @Test
    void validateShouldAcceptTransactionStatusIgnoringCase() {
        RewardTransaction trx = RewardTransaction.builder()
                .status("invoiced")
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(RewardBatchStatus.CREATED)
                .build();

        StepVerifier.create(policy.validate(trx, batch))
                .expectNext(trx)
                .verifyComplete();
    }
}