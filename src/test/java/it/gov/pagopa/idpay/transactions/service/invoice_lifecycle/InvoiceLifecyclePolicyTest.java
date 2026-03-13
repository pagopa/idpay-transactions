package it.gov.pagopa.idpay.transactions.service.invoice_lifecycle;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;

class InvoiceLifecyclePolicyTest {

    private final BasicInvoiceLifecyclePolicy basicPolicy = new BasicInvoiceLifecyclePolicy();
    private final FullInvoiceLifecyclePolicy fullPolicy = new FullInvoiceLifecyclePolicy();

    static Stream<Arguments> policyCombinations() {
        return Stream.of(
                // BatchStatus CREATED
                Arguments.of(RewardBatchStatus.CREATED, RewardBatchTrxStatus.TO_CHECK, true, true),
                Arguments.of(RewardBatchStatus.CREATED, RewardBatchTrxStatus.CONSULTABLE, true, true),
                Arguments.of(RewardBatchStatus.CREATED, RewardBatchTrxStatus.SUSPENDED, true, true),

                // BatchStatus SENT
                Arguments.of(RewardBatchStatus.SENT, RewardBatchTrxStatus.TO_CHECK, false, false),
                Arguments.of(RewardBatchStatus.SENT, RewardBatchTrxStatus.CONSULTABLE, false, false),
                Arguments.of(RewardBatchStatus.SENT, RewardBatchTrxStatus.SUSPENDED, false, false),
                Arguments.of(RewardBatchStatus.SENT, RewardBatchTrxStatus.APPROVED, false, false),
                Arguments.of(RewardBatchStatus.SENT, RewardBatchTrxStatus.REJECTED, false, false),

                // BatchStatus EVALUATING
                Arguments.of(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.TO_CHECK, false, true),
                Arguments.of(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.CONSULTABLE, false, true),
                Arguments.of(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.SUSPENDED, false, true),
                Arguments.of(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.APPROVED, false, false),
                Arguments.of(RewardBatchStatus.EVALUATING, RewardBatchTrxStatus.REJECTED, false, true),

                // BatchStatus APPROVING
                Arguments.of(RewardBatchStatus.APPROVING, RewardBatchTrxStatus.TO_CHECK, false, false),
                Arguments.of(RewardBatchStatus.APPROVING, RewardBatchTrxStatus.CONSULTABLE, false, false),
                Arguments.of(RewardBatchStatus.APPROVING, RewardBatchTrxStatus.SUSPENDED, false, false),
                Arguments.of(RewardBatchStatus.APPROVING, RewardBatchTrxStatus.APPROVED, false, false),
                Arguments.of(RewardBatchStatus.APPROVING, RewardBatchTrxStatus.REJECTED, false, false),

                // BatchStatus APPROVED
                Arguments.of(RewardBatchStatus.APPROVED, RewardBatchTrxStatus.APPROVED, false, false),
                Arguments.of(RewardBatchStatus.APPROVED, RewardBatchTrxStatus.REJECTED, false, true)

                // BatchStatus PENDING_REFUND
//                Arguments.of(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.TO_CHECK, false, false),
//                Arguments.of(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.CONSULTABLE, false, false),
//                Arguments.of(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.SUSPENDED, false, false),
//                Arguments.of(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.APPROVED, false, false),
//                Arguments.of(RewardBatchStatus.PENDING_REFUND, RewardBatchTrxStatus.REJECTED, false, false),

                // BatchStatus NOT_REFUNDED
//                Arguments.of(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.TO_CHECK, false, false),
//                Arguments.of(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.CONSULTABLE, false, false),
//                Arguments.of(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.SUSPENDED, false, false),
//                Arguments.of(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.APPROVED, false, false),
//                Arguments.of(RewardBatchStatus.NOT_REFUNDED, RewardBatchTrxStatus.REJECTED, false, false),

                // BatchStatus REFUNDED
//                Arguments.of(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.TO_CHECK, false, false),
//                Arguments.of(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.CONSULTABLE, false, false),
//                Arguments.of(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.SUSPENDED, false, false),
//                Arguments.of(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.APPROVED, false, false),
//                Arguments.of(RewardBatchStatus.REFUNDED, RewardBatchTrxStatus.REJECTED, false, false),

                );
    }

    @ParameterizedTest(name = "batchStatus={0}, batchTrxStatus={1} -> expectedBasic={2}, expectedFull={3}")
    @MethodSource("policyCombinations")
    void policyValidate(RewardBatchStatus batchStatus, RewardBatchTrxStatus batchTrxStatus, boolean expectedBasic, boolean expectedFull) {

        System.out.printf("[policyValidate] batchStatus=%s, batchTrxStatus=%s, expectedBasic=%s, expectedFull=%s%n",
                batchStatus, batchTrxStatus, expectedBasic, expectedFull);

        SyncTrxStatus trxStatus = batchStatus==RewardBatchStatus.CREATED ?
                SyncTrxStatus.INVOICED : SyncTrxStatus.REWARDED;
        RewardTransaction trx = RewardTransaction.builder()
                .status(trxStatus.name())
                .rewardBatchTrxStatus(batchTrxStatus)
                .build();

        RewardBatch batch = RewardBatch.builder()
                .status(batchStatus)
                .build();

        // check basic policy

        if (expectedBasic) {
            StepVerifier.create(basicPolicy.validate(trx, batch))
                    .expectNext(trx)
                    .verifyComplete();
        } else {
            StepVerifier.create(basicPolicy.validate(trx, batch))
                    .expectError()
                    .verify();
        }

        // check full policy

        if (expectedFull) {
            StepVerifier.create(fullPolicy.validate(trx, batch))
                    .expectNext(trx)
                    .verifyComplete();
        } else {
            StepVerifier.create(fullPolicy.validate(trx, batch))
                    .expectError()
                    .verify();
        }

    }

}
