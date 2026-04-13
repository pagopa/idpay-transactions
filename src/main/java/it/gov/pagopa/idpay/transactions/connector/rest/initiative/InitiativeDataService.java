package it.gov.pagopa.idpay.transactions.connector.rest.initiative;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeData;
import it.gov.pagopa.idpay.transactions.utils.JwtUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class InitiativeDataService {

    private final InitiativeRestClient initiativeRestClient;
    private final Map<String, InitiativeData> initiativeCache = new ConcurrentHashMap<>();

    public InitiativeDataService(InitiativeRestClient initiativeRestClient) {
        this.initiativeRestClient = initiativeRestClient;
    }

    public Mono<InitiativeData> getInitiativeData(String authorization, String initiativeId) {
        InitiativeData cachedValue = initiativeCache.get(initiativeId);

        if (cachedValue != null) {
            log.debug("Cache hit for initiativeId={}", initiativeId);
            return Mono.just(cachedValue);
        }

        log.debug("Cache miss for initiativeId={}", initiativeId);

        String organizationId = JwtUtils.extractOrganizationIdOrThrow(authorization);

        return initiativeRestClient.getInitiative(authorization, organizationId, initiativeId)
                .doOnNext(initiativeData -> {
                    initiativeCache.put(initiativeId, initiativeData);
                    log.debug("Cached initiative data for initiativeId={}", initiativeId);
                });
    }

    void putInCache(String initiativeId, InitiativeData initiativeData) {
        initiativeCache.put(initiativeId, initiativeData);
    }
}
