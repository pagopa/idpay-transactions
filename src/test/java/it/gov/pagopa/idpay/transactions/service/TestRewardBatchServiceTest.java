package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.model.PreparedRewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTestSupportPort;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class TestRewardBatchServiceTest {

    @Mock
    private RewardBatchTestSupportPort port;

    private TestRewardBatchService service;

    @BeforeEach
    void setUp() {
        service = new TestRewardBatchService(port, 18);
    }

    @Test
    void mapsPreparedBatchAndPassesConfiguredSearchHorizon() {
        LocalDateTime updateDate = LocalDateTime.of(2026, 8, 28, 10, 15);
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
}
