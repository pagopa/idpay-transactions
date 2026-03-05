package it.gov.pagopa.idpay.transactions.connector.rest.selfcare;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.exception.SelfcareConnectingErrorException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
@Slf4j
public class SelfcareInstitutionsRestClientImpl implements SelfcareInstitutionsRestClient {

    private final ConcurrentMap<String, Mono<InstitutionList>> institutionsCache = new ConcurrentHashMap<>();


    private final Integer maxAttempts;
    private final Integer retryDelay;

    private final WebClient webClient;


    public SelfcareInstitutionsRestClientImpl(@Value("${app.selfcare.retry.max-attempts}") Integer maxAttempts,
                                              @Value("${app.selfcare.retry.delay-millis}") Integer retryDelay,
                                              @Value("${app.selfcare.institutions-url}") String institutionsUrl,
                                              WebClient.Builder webClientBuilder) {
        this.maxAttempts = maxAttempts;
        this.retryDelay = retryDelay;
        this.webClient = webClientBuilder.clone()
                .baseUrl(institutionsUrl)
                .build();
    }

    @Override
    public Mono<InstitutionList> getInstitutions(String merchantFiscalCode) {

        return institutionsCache.computeIfAbsent(merchantFiscalCode, k ->
                PerformanceLogger.logTimingOnNext(
                        "SELFCARE_GET_INSTITUTIONS",
                        webClient.method(HttpMethod.GET)
                                .uri(uriBuilder -> uriBuilder
                                        .queryParam("taxCode", k)
                                        .build()
                                )
                                .retrieve()
                                .bodyToMono(InstitutionList.class)
                                .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                                        .onRetryExhaustedThrow((spec, signal) -> {
                                            Throwable failure = signal.failure();
                                            log.error("[SELFCARE_GET_INSTITUTIONS] Error to retrieve merchant information: {}", failure.getMessage());
                                            return new SelfcareConnectingErrorException(
                                                    "Failed to retrieve merchant information after " + (maxAttempts + 1) + " attempts",
                                                    failure
                                            );
                                        })
                                )
                                .cache(Duration.ofHours(1)),
                        null
                )
        );
    }
}
