package it.gov.pagopa.idpay.transactions.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.InitiativeRestClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static it.gov.pagopa.idpay.transactions.utils.RewardBatchControlThreshold.validate;

@Service
@Slf4j
@RequiredArgsConstructor
public class InitiativeDataService {

    private final InitiativeRestClient initiativeRestClient;
    private final Map<String, InitiativeDetailDTO> initiativeCache = new ConcurrentHashMap<>();


    public Mono<InitiativeDetailDTO> getInitiativeData(String initiativeId) {
        log.info("[INITIATIVE_DATA_SERVICE] Getting initiative data for initiativeId={}", initiativeId);

        InitiativeDetailDTO cachedValue = initiativeCache.get(initiativeId);

        if (cachedValue != null) {
            log.info("[INITIATIVE_DATA_SERVICE] Cache hit for initiativeId={}, endDate={}", initiativeId, cachedValue.getFruitionEndDate());
            return Mono.just(cachedValue);
        }

        log.info("[INITIATIVE_DATA_SERVICE] Cache miss for initiativeId={}, calling remote service", initiativeId);


        return initiativeRestClient.getInitiativeBeneficiaryDetail(initiativeId)
                .doOnNext(initiativeData -> {
                    initiativeCache.put(initiativeId, initiativeData);
                    log.info("[INITIATIVE_DATA_SERVICE] Cached initiative data for initiativeId={}, endDate={}",
                            initiativeId, initiativeData.getFruitionEndDate());
                })
                .doOnError(error -> log.error("[INITIATIVE_DATA_SERVICE] Error retrieving initiative data for initiativeId={}", initiativeId, error));
    }

    /** Reads the current policy without using the long-lived initiative cache. */
    public Mono<Integer> getRewardBatchControlThreshold(String initiativeId) {
        return initiativeRestClient.getInitiativeBeneficiaryDetail(initiativeId)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Initiative service returned no data for initiativeId=" + initiativeId
                )))
                .flatMap(initiative -> initiative.getRewardBatchControlThreshold() == null
                        ? Mono.error(new IllegalStateException(
                                "Initiative " + initiativeId + " does not support reward batches"
                        ))
                        : Mono.just(validate(initiative.getRewardBatchControlThreshold())))
                .doOnNext(threshold -> log.info(
                        "[INITIATIVE_DATA_SERVICE] Reward batch control threshold for initiativeId={} is {}%",
                        initiativeId,
                        threshold
                ));
    }
}
