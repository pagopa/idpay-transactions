package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.PrepareRewardBatchForSendResponse;
import it.gov.pagopa.idpay.transactions.service.TestRewardBatchService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.Month;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockUser;

@WebFluxTest(
        controllers = TestRewardBatchController.class,
        properties = "app.test-support.enabled=true"
)
class TestRewardBatchControllerTest {

    private static final String PATH =
            "/idpay/internal/test-support/initiatives/initiative-1/reward-batches/batch-1/prepare-for-send";

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private TestRewardBatchService service;

    @MockitoBean
    private CacheManager cacheManager;

    @Test
    void prepareForSendReturnsPreparedBatchWithoutRequestBody() {
        LocalDateTime updateDate = LocalDateTime.of(2026, Month.AUGUST, 28, 11, 30);
        when(service.prepareForSend("initiative-1", "batch-1"))
                .thenReturn(Mono.just(new PrepareRewardBatchForSendResponse(
                        "batch-1", "2026-08", "2026-07", updateDate
                )));

        webTestClient.mutateWith(mockUser()).mutateWith(csrf())
                .post()
                .uri(PATH)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.rewardBatchId").isEqualTo("batch-1")
                .jsonPath("$.previousMonth").isEqualTo("2026-08")
                .jsonPath("$.referenceMonth").isEqualTo("2026-07")
                .jsonPath("$.updateDate").isEqualTo("2026-08-28T11:30:00");

        verify(service).prepareForSend("initiative-1", "batch-1");
    }

    @Test
    void cleanupPassesInitiativeMerchantAndBatchIdsInOrderToService() {
        String cleanupPath =
                "/idpay/internal/test-support/initiatives/initiative-1/reward-batches/batch-1/cleanup";
        when(service.cleanupOldRewardBatchAndRelatedTransactions("initiative-1", "merchant-1", "batch-1"))
                .thenReturn(Mono.empty());

        webTestClient.mutateWith(mockUser()).mutateWith(csrf())
                .delete()
                .uri(cleanupPath)
                .header("x-merchant-id", "merchant-1")
                .exchange()
                .expectStatus().isNoContent();

        verify(service).cleanupOldRewardBatchAndRelatedTransactions("initiative-1", "merchant-1", "batch-1");
    }
}
