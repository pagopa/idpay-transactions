package it.gov.pagopa.idpay.transactions.connector.rest.initiative;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.config.InitiativeClientException;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeData;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailResponseDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Configuration
public class InitiativeRestClientImpl implements InitiativeRestClient {

    private final WebClient initiativeClient;

    public InitiativeRestClientImpl(
            WebClient.Builder webClientBuilder,
            @Value("${app.initiative.base-url}") String baseUrl
    ) {
        this.initiativeClient = webClientBuilder.clone()
                .baseUrl(baseUrl)
                .build();
    }

    @Override
    public Mono<InitiativeData> getInitiative(String authorization, String organizationId, String initiativeId) {
        return PerformanceLogger.logTimingOnNext(
                "INITIATIVE_GET_DETAIL",
                initiativeClient.method(HttpMethod.GET)
                        .uri("/organization/%s/initiative/%s".formatted(organizationId, initiativeId))
                        .header(HttpHeaders.AUTHORIZATION, authorization)
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
                        .bodyToMono(InitiativeDetailResponseDTO.class)
                        .map(this::toInitiativeData),
                null
        );
    }

    private InitiativeData toInitiativeData(InitiativeDetailResponseDTO response) {
        if (response == null || response.getGeneral() == null) {
            throw new InitiativeClientException("Invalid initiative response: missing general section");
        }

        return new InitiativeData(
                response.getInitiativeId(),
                response.getGeneral().getStartDate(),
                response.getGeneral().getEndDate());
    }
}
