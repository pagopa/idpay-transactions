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
        log.info("[INITIATIVE_DATA_SERVICE] Getting initiative data for initiativeId={}", initiativeId);

        InitiativeData cachedValue = initiativeCache.get(initiativeId);

        if (cachedValue != null) {
            log.info("[INITIATIVE_DATA_SERVICE] Cache hit for initiativeId={}, endDate={}", initiativeId, cachedValue.initiativeEndDate());
            return Mono.just(cachedValue);
        }

        log.info("[INITIATIVE_DATA_SERVICE] Cache miss for initiativeId={}, calling remote service", initiativeId);

        String organizationId = JwtUtils.extractOrganizationIdOrThrow(authorization);
        log.info("[INITIATIVE_DATA_SERVICE] Extracted organizationId={} from JWT", organizationId);

        return initiativeRestClient.getInitiative(authorization, organizationId, initiativeId)
                .doOnNext(initiativeData -> {
                    initiativeCache.put(initiativeId, initiativeData);
                    log.info("[INITIATIVE_DATA_SERVICE] Cached initiative data for initiativeId={}, startDate={}, endDate={}",
                            initiativeId, initiativeData.initiativeStartDate(), initiativeData.initiativeEndDate());
                })
                .doOnError(error -> log.error("[INITIATIVE_DATA_SERVICE] Error retrieving initiative data for initiativeId={}", initiativeId, error));
    }
}
