package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.PaymentRestClient;
import it.gov.pagopa.idpay.transactions.model.PreparedRewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTestSupportPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionTestSupportPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
@ExtendWith(MockitoExtension.class)
class TestRewardBatchServiceTest {

    @Mock
    private RewardBatchTestSupportPort port;
    @Mock
    private RewardTransactionTestSupportPort transactionPort;
    @Mock
    private PaymentRestClient paymentRestClient;

    private TestRewardBatchService service;

    @BeforeEach
    void setUp() {
        service = new TestRewardBatchService(port, 18, paymentRestClient, transactionPort);
    }

    @Test
    void mapsPreparedBatchAndPassesConfiguredSearchHorizon() {
        LocalDateTime updateDate = LocalDateTime.of(2026, Month.AUGUST, 28, 10, 15);
        when(port.prepareForSend("initiative", "batch", 18))
                .thenReturn(Mono.just(new PreparedRewardBatch(
                        "batch", "2026-08", "2026-07", updateDate
                )));

        StepVerifier.create(service.prepareForSend("initiative", "batch"))
                .assertNext(response -> {
                    org.junit.jupiter.api.Assertions.assertEquals("batch", response.rewardBatchId());
                    org.junit.jupiter.api.Assertions.assertEquals("2026-08", response.previousMonth());
                    org.junit.jupiter.api.Assertions.assertEquals("2026-07", response.referenceMonth());
                    org.junit.jupiter.api.Assertions.assertEquals(updateDate, response.updateDate());
                })
                .verifyComplete();

        verify(port).prepareForSend("initiative", "batch", 18);
    }

    @Test
    void propagatesPortFailure() {
        IllegalStateException failure = new IllegalStateException("database unavailable");
        when(port.prepareForSend("initiative", "batch", 18)).thenReturn(Mono.error(failure));

        StepVerifier.create(service.prepareForSend("initiative", "batch"))
                .expectErrorSatisfies(error -> assertSame(failure, error))
                .verify();
    }

    @Test
    void cleanupDeletesTransactionsAndBatchWhenTransactionsExist() {
        when(transactionPort.deleteByRewardBatchIdAndInitiativeIdReturningIds("batch", "initiative"))
                .thenReturn(Flux.just("trx-1", "trx-2"));
        when(paymentRestClient.cleanupTransactions("initiative", Set.of("trx-1", "trx-2")))
                .thenReturn(Mono.empty());
        when(port.cleanupRewardBatch("initiative", "merchant", "batch"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.cleanupOldRewardBatchAndRelatedTransactions(
                        "initiative", "merchant", "batch"
                ))
                .verifyComplete();

        verify(transactionPort).deleteByRewardBatchIdAndInitiativeIdReturningIds("batch", "initiative");
        verify(paymentRestClient).cleanupTransactions("initiative", Set.of("trx-1", "trx-2"));
        verify(port).cleanupRewardBatch("initiative", "merchant", "batch");
    }

    @Test
    void cleanupSkipsPaymentCallWhenNoTransactionsAreAssigned() {
        when(transactionPort.deleteByRewardBatchIdAndInitiativeIdReturningIds("batch", "initiative"))
                .thenReturn(Flux.empty());
        when(port.cleanupRewardBatch("initiative", "merchant", "batch"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.cleanupOldRewardBatchAndRelatedTransactions(
                        "initiative", "merchant", "batch"
                ))
                .verifyComplete();

        verify(paymentRestClient, never()).cleanupTransactions(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anySet());
        verify(port).cleanupRewardBatch("initiative", "merchant", "batch");
    }

    @Test
    void cleanupRewardBatchFailsReturnLog(CapturedOutput output) {
        IllegalStateException failure = new IllegalStateException("database unavailable");
        when(transactionPort.deleteByRewardBatchIdAndInitiativeIdReturningIds("batch", "initiative"))
                .thenReturn(Flux.empty());
        when(port.cleanupRewardBatch("initiative", "merchant", "batch"))
                .thenReturn(Mono.error(failure));

        StepVerifier.create(service.cleanupOldRewardBatchAndRelatedTransactions(
                        "initiative", "merchant", "batch"
                ))
                .expectErrorSatisfies(error -> assertSame(failure, error))
                .verify();

        verify(paymentRestClient, never()).cleanupTransactions(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anySet());
        verify(port).cleanupRewardBatch("initiative", "merchant", "batch");
        assertTrue(output.getOut().contains(
                "[TEST_SUPPORT_CLEANUP_REWARD_BATCH] Error cleaning up rewardBatchId=batch, initiativeId=initiative, merchantId=merchant"
        ));
    }

}
