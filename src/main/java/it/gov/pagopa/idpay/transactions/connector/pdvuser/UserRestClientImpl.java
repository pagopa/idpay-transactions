package it.gov.pagopa.idpay.transactions.connector.pdvuser;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.connector.pdvuser.dto.CFDTO;
import it.gov.pagopa.idpay.transactions.connector.pdvuser.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.pdvuser.dto.UserInfoPDV;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

@Service
@Slf4j
public class UserRestClientImpl implements UserRestClient {
    private static final String API_KEY_HEADER = "x-api-key";
    private static final String URI_DECRYPT = "/tokens/{token}/pii";
    private static final String URI_ENCRYPT = "/tokens";
    private final int pdvRetryDelay;
    private final long pdvMaxAttempts;
    private final WebClient webClient;

    public UserRestClientImpl(@Value("${app.pdv.base-url}") String pdvBaseUrl,
                              @Value("${app.pdv.headers.x-api-key}") String apiKeyValue,
                              @Value("${app.pdv.retry.delay-millis}") int pdvRetryDelay,
                              @Value("${app.pdv.retry.max-attempts}") long pdvMaxAttempts,
                              WebClient.Builder webClientBuilder) {
        this.pdvRetryDelay = pdvRetryDelay;
        this.pdvMaxAttempts = pdvMaxAttempts;
        this.webClient = webClientBuilder.clone()
                .baseUrl(pdvBaseUrl)
                .defaultHeader(API_KEY_HEADER, apiKeyValue)
                .build();
    }

    @Override
    public Mono<UserInfoPDV> retrieveUserInfo(String userId) {
        return PerformanceLogger.logTimingOnNext(
                        "PDV_INTEGRATION",
                        webClient
                                .method(HttpMethod.GET)
                                .uri(URI_DECRYPT, Map.of("token", userId))
                                .retrieve()
                                .toEntity(UserInfoPDV.class),
                        x -> "httpStatus %s".formatted(x.getStatusCode().value())
                )
                .map(HttpEntity::getBody)
                .retryWhen(Retry.fixedDelay(pdvMaxAttempts, Duration.ofMillis(pdvRetryDelay))
                        .filter(ex -> {
                            boolean retry = (ex instanceof WebClientResponseException.TooManyRequests) || ex.getMessage().startsWith("Connection refused");
                            if (retry) {
                                log.info("[PDV_INTEGRATION] Retrying invocation due to exception: {}: {}", ex.getClass().getSimpleName(), ex.getMessage());
                            }
                            return retry;
                        })
                )

                .onErrorResume(WebClientResponseException.NotFound.class, x -> {
                    log.warn("userId not found into pdv: {}", userId);
                    return Mono.empty();
                })
                .onErrorResume(WebClientResponseException.BadRequest.class, x -> {
                    log.warn("userId not valid: {}", userId);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<FiscalCodeInfoPDV> retrieveFiscalCodeInfo(String fiscalCode) {
        CFDTO cfdto = new CFDTO(fiscalCode);
        return PerformanceLogger.logTimingOnNext(
                        "PDV_INTEGRATION",
                        webClient
                                .method(HttpMethod.PUT)
                                .uri(URI_ENCRYPT).accept(MediaType.APPLICATION_JSON)
                                .bodyValue(cfdto)
                                .retrieve()
                                .toEntity(FiscalCodeInfoPDV.class),
                        x -> "httpStatus %s".formatted(x.getStatusCode().value())
                )
                .map(HttpEntity::getBody)
                .retryWhen(Retry.fixedDelay(pdvMaxAttempts, Duration.ofMillis(pdvRetryDelay))
                        .filter(ex -> {
                            boolean retry = (ex instanceof WebClientResponseException.TooManyRequests) || ex.getMessage().startsWith("Connection refused");
                            if (retry) {
                                log.info("[PDV_INTEGRATION] Retrying invocation due to exception: {}: {}", ex.getClass().getSimpleName(), ex.getMessage());
                            }
                            return retry;
                        })
                )

                .onErrorResume(WebClientResponseException.NotFound.class, x -> {
                    log.warn("userId not found into pdv: {}", fiscalCode);
                    return Mono.empty();
                })
                .onErrorResume(WebClientResponseException.BadRequest.class, x -> {
                    log.warn("userId not valid: {}", fiscalCode);
                    return Mono.empty();
                });
    }

}
