package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.PaymentRestClient;
import it.gov.pagopa.idpay.transactions.dto.PrepareRewardBatchForSendResponse;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTestSupportPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionTestSupportPort;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;

@Service
@Slf4j
@ConditionalOnProperty(name = "app.test-support.enabled", havingValue = "true")
public class TestRewardBatchService {

    private final RewardBatchTestSupportPort rewardBatchTestSupportPort;
    private final int searchHorizonMonths;
    private final PaymentRestClient paymentRestClient;
    private final RewardTransactionTestSupportPort rewardTransactionTestSupportPort;

    public TestRewardBatchService(
            RewardBatchTestSupportPort rewardBatchTestSupportPort,
            @Value("${app.test-support.reward-batch-search-horizon-months:120}") int searchHorizonMonths,
            PaymentRestClient paymentRestClient,
            RewardTransactionTestSupportPort rewardTransactionTestSupportPort
    ) {
        this.rewardBatchTestSupportPort = rewardBatchTestSupportPort;
        this.searchHorizonMonths = searchHorizonMonths;
        this.paymentRestClient = paymentRestClient;
        this.rewardTransactionTestSupportPort = rewardTransactionTestSupportPort;
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

    public Mono<Void> cleanupOldRewardBatchAndRelatedTransactions(String initiativeId, String merchantId, String rewardBatchId) {
        return rewardTransactionTestSupportPort.deleteByRewardBatchIdAndInitiativeIdReturningIds(
                        rewardBatchId,
                        initiativeId
                )
                .collect(Collectors.toSet())
                .flatMap(transactionIds -> transactionIds.isEmpty()
                        ? Mono.empty()
                        : paymentRestClient.cleanupTransactions(initiativeId, transactionIds))
                .then(rewardBatchTestSupportPort.cleanupRewardBatch(
                        initiativeId,
                        merchantId,
                        rewardBatchId
                ))
                .doOnError(e -> log.error(
                        "[TEST_SUPPORT_CLEANUP_REWARD_BATCH] Error cleaning up rewardBatchId={}, initiativeId={}, merchantId={}",
                        Utilities.sanitizeString(rewardBatchId),
                        Utilities.sanitizeString(initiativeId),
                        Utilities.sanitizeString(merchantId), e));
    }
}
