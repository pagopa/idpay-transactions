package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.PrepareRewardBatchForSendResponse;
import it.gov.pagopa.idpay.transactions.service.TestRewardBatchService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/idpay/internal/test-support")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.test-support.enabled", havingValue = "true")
public class TestRewardBatchController {

    private final TestRewardBatchService testRewardBatchService;

    @PostMapping(
            "/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/prepare-for-send"
    )
    public Mono<PrepareRewardBatchForSendResponse> prepareForSend(
            @PathVariable String initiativeId,
            @PathVariable String rewardBatchId
    ) {
        return testRewardBatchService.prepareForSend(initiativeId, rewardBatchId);
    }
}
