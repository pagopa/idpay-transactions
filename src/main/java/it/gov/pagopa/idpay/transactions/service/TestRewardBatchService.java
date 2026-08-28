package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.PrepareRewardBatchForSendResponse;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTestSupportPort;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@ConditionalOnProperty(name = "app.test-support.enabled", havingValue = "true")
public class TestRewardBatchService {

    private final RewardBatchTestSupportPort rewardBatchTestSupportPort;
    private final int searchHorizonMonths;

    public TestRewardBatchService(
            RewardBatchTestSupportPort rewardBatchTestSupportPort,
            @Value("${app.test-support.reward-batch-search-horizon-months:120}")
            int searchHorizonMonths
    ) {
        this.rewardBatchTestSupportPort = rewardBatchTestSupportPort;
        this.searchHorizonMonths = searchHorizonMonths;
    }

    public Mono<PrepareRewardBatchForSendResponse> prepareForSend(
            String initiativeId,
            String rewardBatchId
    ) {
        return rewardBatchTestSupportPort.prepareForSend(
                        initiativeId,
                        rewardBatchId,
                        searchHorizonMonths
                )
                .doOnNext(prepared -> log.info(
                        "[TEST_SUPPORT_PREPARE_REWARD_BATCH] Prepared rewardBatchId={}, initiativeId={}, oldMonth={}, referenceMonth={}",
                        Utilities.sanitizeString(prepared.rewardBatchId()),
                        Utilities.sanitizeString(initiativeId),
                        Utilities.sanitizeString(prepared.previousMonth()),
                        Utilities.sanitizeString(prepared.referenceMonth())
                ))
                .map(prepared -> new PrepareRewardBatchForSendResponse(
                        prepared.rewardBatchId(),
                        prepared.previousMonth(),
                        prepared.referenceMonth(),
                        prepared.updateDate()
                ));
    }
}
