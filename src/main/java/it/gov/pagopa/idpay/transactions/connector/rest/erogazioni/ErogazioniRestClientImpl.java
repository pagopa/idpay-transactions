package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;

import static it.gov.pagopa.idpay.transactions.utils.Utilities.sanitizeString;

@Service
@Slf4j
public class ErogazioniRestClientImpl implements ErogazioniRestClient {
    private static final String URI_EROGAZIONI = "/erogazioni";
    private static final String URI_ESITI = "/esiti";
    private static final String REQUEST_PARAM_ESITI = "idRichiesta";
    public static final String VAT_NUMBER_DEFAULT = "00000000000";

    private final InvitaliaTokenProviderService tokenProvider;
    private final WebClient webClient;

    private final Integer maxAttempts;
    private final Integer retryDelay;
    private final String autorizzatore;

    private final JsonMapper objectMapper;

    public ErogazioniRestClientImpl(InvitaliaTokenProviderService tokenProvider,
                                    @Value("${app.erogazioni.retry.max-attempts:3}") Integer maxAttempts,
                                    @Value("${app.erogazioni.retry.delay-millis:500}") Integer retryDelay,
                                    @Value("${app.erogazioni.erogazioni-url}") String erogazioniBaseUrl,
                                    @Value("${app.erogazioni.authorizer:}") String autorizzatore,
                                    WebClient.Builder webClientBuilder,
                                    JsonMapper objectMapper) {
        this.tokenProvider = tokenProvider;
        this.maxAttempts = maxAttempts;
        this.retryDelay = retryDelay;
        this.autorizzatore = autorizzatore;
        this.webClient = webClientBuilder.clone()
                .baseUrl(erogazioniBaseUrl)
                .build();
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<DeliveryOutcomeDTO> postErogazione(DeliveryRequest deliveryRequest) {
        if (deliveryRequest.getAnagrafica() != null) {
            deliveryRequest.getAnagrafica().setPartitaIvaCliente(
                    formatPartitaIva(deliveryRequest.getAnagrafica().getPartitaIvaCliente())
            );

            deliveryRequest.getAnagrafica()
                    .setRagioneSocialeIntestatario(StringUtils.truncate(deliveryRequest.getAnagrafica().getRagioneSocialeIntestatario(), 140));
        }

        if (deliveryRequest.getErogazione() != null) {
            deliveryRequest.getErogazione().setAutorizzatore(this.autorizzatore);
        }

        log.info("[POST_EROGAZIONE] Sending delivery request for batchId: {}",
                sanitizeString(deliveryRequest.getId()));

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
                                sanitizeString(deliveryRequest.getId()), outcome.isSucceded()))

                        .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                                .filter(ex -> {
                                    if (ex instanceof WebClientResponseException wcre) {
                                        return !wcre.getStatusCode().is4xxClientError();
                                    }
                                    return true;
                                })
                                .onRetryExhaustedThrow((spec, signal) -> {
                                    log.error("[POST_EROGAZIONE] Max attempts reached for id: {}",
                                            sanitizeString(deliveryRequest.getId()));
                                    return new RuntimeException("Retry exhausted after technical failures", signal.failure());
                                })
                        )
                )
                .onErrorResume(e -> {
                    String detailedMessage = e.getMessage();
                    if (e.getCause() != null) {
                        detailedMessage = e.getCause().getMessage();
                    }

                    log.error("[POST_EROGAZIONE] Permanent failure for batch {}: {}",
                            sanitizeString(deliveryRequest.getId()), detailedMessage);

                    return Mono.just(DeliveryOutcomeDTO.builder()
                            .succeded(false)
                            .message("Technical error: " + detailedMessage)
                            .timestamp(LocalDateTime.now())
                            .build());
                });
    }

    @Override
    public Mono<InvitaliaOutcomeResponseDTO> getOutcome(String requestId) {

        log.info("[GET_OUTCOME] Fetching Invitalia outcome for requestId: {}", sanitizeString(requestId));

        return tokenProvider.retrieveToken()
                .flatMap(token -> webClient.get()
                        .uri(uriBuilder -> uriBuilder
                                .path(URI_ESITI)
                                .queryParam(REQUEST_PARAM_ESITI, requestId)
                                .build())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .retrieve()
                        .onStatus(HttpStatusCode::is4xxClientError, response ->
                                response.bodyToMono(String.class)
                                        .flatMap(body -> {
                                            log.warn(
                                                    "[GET_OUTCOME] Invitalia returned {} for requestId {}. Response body: {}",
                                                    response.statusCode(),
                                                    sanitizeString(requestId),
                                                    sanitizeString(body)
                                            );
                                            return Mono.empty();
                                        })
                        )

                        .bodyToMono(String.class)
                        .flatMap(body -> {
                            try {
                                InvitaliaOutcomeResponseDTO dto = objectMapper.readValue(body, InvitaliaOutcomeResponseDTO.class);
                                return Mono.just(dto);
                            } catch (Exception e) {
                                return Mono.error(new RuntimeException("Error deserializing Invitalia outcome", e));
                            }
                        })
                        .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                                .doBeforeRetry(signal ->
                                        log.warn("[GET_OUTCOME] Retry attempt {} for requestId: {} due to {}",
                                                signal.totalRetries() + 1,
                                                sanitizeString(requestId),
                                                signal.failure().getMessage())
                                )
                        )
                )
                .doOnSuccess(resp -> log.info("[GET_OUTCOME] Successfully fetched outcome for requestId: {}", sanitizeString(requestId)))
                .doOnError(err -> log.error("[GET_OUTCOME] Error fetching outcome for requestId: {}", sanitizeString(requestId), err));
    }

    private String formatPartitaIva(String piva) {
        if (piva != null && piva.length() == 16) {
            return VAT_NUMBER_DEFAULT;
        }
        return piva;
    }
}