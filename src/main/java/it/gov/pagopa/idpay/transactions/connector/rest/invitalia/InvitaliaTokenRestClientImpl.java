package it.gov.pagopa.idpay.transactions.connector.rest.invitalia;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.config.InvitaliaConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.TokenDTO;
import it.gov.pagopa.idpay.transactions.exception.InvitaliaConnectingErrorException;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
public class InvitaliaTokenRestClientImpl implements InvitaliaTokenRestClient {
    private final MultiValueMap<String, String> tokenBody;
    private final Integer maxAttemptsToken;
    private final Integer retryDelayToken;
    private final String tenant;
    private final String path;

    private final WebClient tokenClient;
    private final AuditUtilities auditUtilities;

    public InvitaliaTokenRestClientImpl(InvitaliaConfig invitaliaConfig,
                                        WebClient.Builder webClientBuilder, AuditUtilities auditUtilities) {
        this.auditUtilities = auditUtilities;

        this.tokenBody = new LinkedMultiValueMap<>();
        InvitaliaConfig.Credentials credentials = invitaliaConfig.getCredentials();
        this.tokenBody.add("client_id", credentials.getClientId());
        this.tokenBody.add("client_secret", credentials.getClientSecret());
        this.tokenBody.add("scope", credentials.getScope());
        this.tokenBody.add("grant_type", credentials.getGrantType());

        InvitaliaConfig.Token token = invitaliaConfig.getToken();
        this.maxAttemptsToken = token.getRetry().getMaxAttempts();
        this.retryDelayToken = token.getRetry().getDelayMillis();
        this.tenant = credentials.getTenantId();
        this.path = token.getPath();
        this.tokenClient = webClientBuilder.clone()
                .baseUrl(token.getBaseUrl())
                .build();
    }

    @Override
    public Mono<TokenDTO> getToken() {
        return PerformanceLogger.logTimingOnNext(
                "INVITALIA_GET_TOKEN",
                tokenClient.method(HttpMethod.GET)
                        .uri("/%s".formatted(tenant).concat(path))
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .body(BodyInserters.fromFormData(tokenBody))
                        .retrieve()
                        .bodyToMono(TokenDTO.class)
                        .retryWhen(Retry.fixedDelay(maxAttemptsToken, Duration.ofMillis(retryDelayToken))
                                .onRetryExhaustedThrow((spec, signal) -> {
                                    Throwable failure = signal.failure();
                                    auditUtilities.logErrorAuth("[INVITALIA_GET_TOKEN] Error to retrieve token: %s".formatted(failure.getMessage()));
                                            return new InvitaliaConnectingErrorException(
                                                    "Failed to retrieve Invitalia token after " + (maxAttemptsToken + 1) + " attempts",
                                                    failure
                                            );
                                        }
                                )
                        ),
                null
        );
    }
}
