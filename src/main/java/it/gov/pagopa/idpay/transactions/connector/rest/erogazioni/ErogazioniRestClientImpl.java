package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import org.springframework.http.HttpStatusCode;
import java.time.Duration;
import java.time.LocalDateTime;

@Service
@Slf4j
public class ErogazioniRestClientImpl implements ErogazioniRestClient {
    private static final String URI_EROGAZIONI = "/erogazioni";

    private final InvitaliaTokenProviderService tokenProvider;
    private final WebClient webClient;

    private final Integer maxAttempts;
    private final Integer retryDelay;
    private final String autorizzatore;

    public ErogazioniRestClientImpl(InvitaliaTokenProviderService tokenProvider,
                                    @Value("${app.erogazioni.retry.max-attempts:3}") Integer maxAttempts,
                                    @Value("${app.erogazioni.retry.delay-millis:500}") Integer retryDelay,
                                    @Value("${app.erogazioni.erogazioni-url}") String erogazioniBaseUrl,
                                    @Value("${app.erogazioni.authorizer:}") String autorizzatore,
                                    WebClient.Builder webClientBuilder) {
        this.tokenProvider = tokenProvider;
        this.maxAttempts = maxAttempts;
        this.retryDelay = retryDelay;
        this.autorizzatore = autorizzatore;
        this.webClient = webClientBuilder.clone()
                .baseUrl(erogazioniBaseUrl)
                .build();
    }

    @Override
    public Mono<DeliveryOutcomeDTO> postErogazione(DeliveryRequest deliveryRequest) {
        if (deliveryRequest.getAnagrafica() != null) {
            deliveryRequest.getAnagrafica().setPartitaIvaCliente(
                    formatPartitaIva(deliveryRequest.getAnagrafica().getPartitaIvaCliente())
            );
        }

        if (deliveryRequest.getErogazione() != null) {
            deliveryRequest.getErogazione().setAutorizzatore(this.autorizzatore);
        }

        log.info("[POST_EROGAZIONE] Sending delivery request for batchId: {}",
                Utilities.sanitizeString(deliveryRequest.getId()));

            return tokenProvider.retrieveToken()
                    .flatMap(token -> webClient.post()
                            .uri(URI_EROGAZIONI)
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .header("Request-Id", deliveryRequest.getId())
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(deliveryRequest)
                            .retrieve()
                            .onStatus(HttpStatusCode::is4xxClientError, response -> Mono.empty())
                            .onStatus(HttpStatusCode::is5xxServerError, response ->
                                    Mono.error(new RuntimeException("Server error during erogazione")))
                            .bodyToMono(DeliveryOutcomeDTO.class)
                            .doOnNext(outcome -> log.info("[POST_EROGAZIONE] Received outcome for batch {}: success={}",
                                    Utilities.sanitizeString(deliveryRequest.getId()), outcome.isSucceded()))

                            .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                                    .filter(ex -> {
                                       if (ex instanceof WebClientResponseException wcre) {
                                           return !wcre.getStatusCode().is4xxClientError();
                                        }
                                        return true;
                                    })
                                    .onRetryExhaustedThrow((spec, signal) -> {
                                        log.error("[POST_EROGAZIONE] Max attempts reached for id: {}",
                                                Utilities.sanitizeString(deliveryRequest.getId()));
                                        return new RuntimeException("Retry exhausted after technical failures", signal.failure());
                                    })
                            )
                    )
                    .onErrorResume(e -> {
                        log.error("[POST_EROGAZIONE] Permanent failure for batch {}: {}",
                                Utilities.sanitizeString(deliveryRequest.getId()), e.getMessage());
                        return Mono.just(DeliveryOutcomeDTO.builder()
                                .succeded(false)
                                .message("Persistent technical error: " + e.getMessage())
                                .timestamp(LocalDateTime.now())
                                .build());
                    });
        }

    private String formatPartitaIva(String piva) {
        if (piva != null && piva.length() == 16) {
            return "00000000000";
        }
        return piva;
    }
}