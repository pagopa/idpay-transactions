package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.exception.ErogazioniConnectingErrorException; // Assicurati che esista o usa una generica
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
public class ErogazioniRestClientImpl implements ErogazioniRestClient {

    private final InvitaliaTokenProviderService tokenProvider;
    private final WebClient webClient;

    private final Integer maxAttempts;
    private final Integer retryDelay;
    private final String autorizzatore;

    public ErogazioniRestClientImpl(InvitaliaTokenProviderService tokenProvider,
                                    @Value("${app.erogazioni.retry.max-attempts:3}") Integer maxAttempts,
                                    @Value("${app.erogazioni.retry.delay-millis:500}") Integer retryDelay,
                                    @Value("${app.erogazioni.base-url}") String erogazioniBaseUrl,
                                    @Value("${app.erogazioni.autorizzatore:gianluca fiorillo}") String autorizzatore,
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
    public Mono<Void> sendErogazione(DeliveryRequest deliveryRequest) {
        // Logica specifica richiesta: se P.IVA è un CF da 16, metti 11 zeri
        deliveryRequest.setPartitaIvaCliente(formatPartitaIva(deliveryRequest.getPartitaIvaCliente()));
        deliveryRequest.setAutorizzatore(this.autorizzatore);

        log.info("[SEND_EROGAZIONE] Sending delivery request for batchId: {} and idPratica: {}",
                deliveryRequest.getId(), deliveryRequest.getIdPratica());

        return tokenProvider.retrieveToken() // Recupero il token Invitalia
                .flatMap(token -> webClient.post()
                        .uri("/erogazioni") // Endpoint visto nello script
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .header("Request-Id", deliveryRequest.getId()) // Come nello script Python
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(deliveryRequest)
                        .retrieve()
                        .toBodilessEntity()
                        .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                                .filter(ex -> !(ex instanceof org.springframework.web.reactive.function.client.WebClientResponseException.BadRequest))
                                .onRetryExhaustedThrow((spec, signal) -> {
                                    log.error("[SEND_EROGAZIONE] Max attempts reached for id: {}", deliveryRequest.getId());
                                    return new RuntimeException("Error sending erogazione after retry", signal.failure());
                                })
                        )
                )
                .doOnSuccess(v -> log.info("[SEND_EROGAZIONE] Successfully sent request for id: {}", deliveryRequest.getId()))
                .then();
    }

    private String formatPartitaIva(String piva) {
        if (piva != null && piva.length() == 16) {
            return "00000000000"; // 11 zeri come richiesto
        }
        return piva;
    }
}