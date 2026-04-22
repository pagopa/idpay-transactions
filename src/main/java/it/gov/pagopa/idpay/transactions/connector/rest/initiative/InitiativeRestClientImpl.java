package it.gov.pagopa.idpay.transactions.connector.rest.initiative;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.config.InitiativeClientException;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class InitiativeRestClientImpl implements InitiativeRestClient {

  private final WebClient initiativeClient;

  public InitiativeRestClientImpl(
      WebClient.Builder webClientBuilder, @Value("${app.initiative.base-url}") String baseUrl) {
    this.initiativeClient = webClientBuilder.clone().baseUrl(baseUrl).build();
  }

  @Override
  public Mono<InitiativeDetailDTO> getInitiativeBeneficiaryDetail(String initiativeId) {
    return PerformanceLogger.logTimingOnNext(
        "GET_INITIATIVE_BENFICIARY_DETAIL",
        initiativeClient
            .method(HttpMethod.GET)
            .uri(
                uriBuilder ->
                    uriBuilder
                        .path("/idpay/initiative/{initiativeId}/detail")
                        .queryParam("viewMinimalInfo", false)
                        .build(initiativeId))
            .retrieve()
            .onStatus(
                status -> status.value() == 404,
                response ->
                    Mono.error(
                        new InitiativeNotFoundException(
                            "Initiative not found: initiativeId=%s".formatted(initiativeId))))
            .onStatus(
                HttpStatusCode::isError,
                response ->
                    Mono.error(
                        new InitiativeClientException(
                            "Error while retrieving initiative detail for initiativeId=%s"
                                .formatted(initiativeId))))
            .bodyToMono(InitiativeDetailDTO.class),
        null);
  }
}
