package it.gov.pagopa.idpay.transactions.connector.rest.selfcare;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.exception.InvitaliaConnectingErrorException;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
public class SelfcareInstitutionsRestClientImpl implements SelfcareInstitutionsRestClient {

    private final Integer maxAttempts;
    private final Integer retryDelay;

    private final WebClient webClient;

    private final AuditUtilities auditUtilities;

    public SelfcareInstitutionsRestClientImpl(@Value("${app.selfcare.retry.max-attempts}") Integer maxAttempts,
                                              @Value("${app.selfcare.retry.delay-millis}") Integer retryDelay,
                                              @Value("${app.selfcare.institutions-url}") String instituitionsUrl,
                                              WebClient.Builder webClientBuilder, AuditUtilities auditUtilities) {
        this.maxAttempts = maxAttempts;
        this.retryDelay = retryDelay;
        this.auditUtilities = auditUtilities;
        this.webClient = webClientBuilder.clone()
                .baseUrl(instituitionsUrl)
                .build();
    }

    @Override
    public Mono<InstitutionList> getInstitutions(String merchantFiscalCode) {
        return PerformanceLogger.logTimingOnNext(
                "SELFCARE_GET_INSTITUTIONS",
                webClient.method(HttpMethod.GET)
                        .uri(uriBuilder -> uriBuilder
                                .queryParam("taxCode", merchantFiscalCode)
                                .build()
                        )
                        .retrieve()
                        .bodyToMono(InstitutionList.class).retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                                .onRetryExhaustedThrow((spec, signal) -> {
                                            Throwable failure = signal.failure();
                                            auditUtilities.logErrorSelfcare("[SELFCARE_GET_INSTITUTIONS] Error to retrieve merchant information: %s".formatted(failure.getMessage()));
                                            return new InvitaliaConnectingErrorException(
                                                    "Failed to retrieve merchant information after " + (maxAttempts + 1) + " attempts",
                                                    failure
                                            );
                                        }
                                )
                        ),
                null
        );
    }
}
