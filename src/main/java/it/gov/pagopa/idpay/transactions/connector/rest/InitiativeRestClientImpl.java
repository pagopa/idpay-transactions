package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.config.InitiativeClientException;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
public class InitiativeRestClientImpl implements InitiativeRestClient {

    private static final String URI_INITIATIVE_DETAIL = "/idpay/initiative/{initiativeId}/detail";

    private final WebClient initiativeClient;
    private final int retryDelay;
    private final long maxAttempts;

    public InitiativeRestClientImpl(
            @Value("${app.initiative.base-url}") String baseUrl,
            @Value("${app.initiative.retry.delay-millis}") int retryDelay,
            @Value("${app.initiative.retry.max-attempts}") long maxAttempts,
            WebClient.Builder webClientBuilder
    ) {
        this.retryDelay = retryDelay;
        this.maxAttempts = maxAttempts;
        this.initiativeClient = webClientBuilder.clone()
                .baseUrl(baseUrl)
                .build();
    }

    @Override
    public Mono<InitiativeDetailDTO> getInitiativeBeneficiaryDetail(String initiativeId) {
        log.info("Sending get initiative beneficiary detail request for initiativeId {}", initiativeId);

        return PerformanceLogger.logTimingOnNext(
                "GET_INITIATIVE_BENEFICIARY_DETAIL",
                initiativeClient
                        .method(HttpMethod.GET)
                        .uri(uriBuilder -> uriBuilder
                                .path(URI_INITIATIVE_DETAIL)
                                .queryParam("viewMinimalInfo", false)
                                .build(initiativeId))
                        .retrieve()
                        .onStatus(
                                status -> status.value() == 404,
                                response -> Mono.error(
                                        new InitiativeNotFoundException(
                                                "Initiative not found: initiativeId=%s".formatted(initiativeId)
                                        )
                                )
                        )
                        .onStatus(
                                HttpStatusCode::isError,
                                response -> Mono.error(
                                        new InitiativeClientException(
                                                "Error while retrieving initiative detail for initiativeId=%s"
                                                        .formatted(initiativeId)
                                        )
                                )
                        )
                        .bodyToMono(InitiativeDetailDTO.class),
                null
        )
                .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                        .filter(ex -> {
                            boolean retry = ex instanceof InitiativeClientException;

                            if (retry) {
                                log.info("[GET_INITIATIVE_BENEFICIARY_DETAIL] Retrying invocation due to exception: {}: {}",
                                        ex.getClass().getSimpleName(), ex.getMessage());
                            }

                            return retry;
                        })
                )
                .onErrorResume(InitiativeNotFoundException.class, ex -> {
                    log.warn("[GET_INITIATIVE_BENEFICIARY_DETAIL] Initiative {} not found", initiativeId);
                    return Mono.error(ex);
                })
                .onErrorResume(InitiativeClientException.class, ex -> {
                    log.warn("[GET_INITIATIVE_BENEFICIARY_DETAIL] Error retrieving initiative detail for initiativeId {}", initiativeId);
                    return Mono.error(ex);
                });
    }

}
