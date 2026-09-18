package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.UpdateTransactionsStatusRequestDTO;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import java.time.Duration;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
@Slf4j
public class PaymentRestClientImpl implements PaymentRestClient {

    private static final String URI_UPDATE_TRANSACTIONS_STATUS = "/idpay/transactions/status";

    private final WebClient paymentClient;
    private final int retryDelay;
    private final long maxAttempts;

    public PaymentRestClientImpl(
            @Value("${app.payment.base-url}") String baseUrl,
            @Value("${app.payment.retry.delay-millis}") int retryDelay,
            @Value("${app.payment.retry.max-attempts}") long maxAttempts,
            WebClient.Builder webClientBuilder
    ) {
        this.retryDelay = retryDelay;
        this.maxAttempts = maxAttempts;
        this.paymentClient = webClientBuilder.clone()
                .baseUrl(baseUrl)
                .build();
    }

    @Override
    public Mono<Integer> updateTransactionsStatus(Set<String> transactionIds, SyncTrxStatus status) {
        UpdateTransactionsStatusRequestDTO request = UpdateTransactionsStatusRequestDTO.builder()
                .transactionIds(transactionIds)
                .status(status)
                .build();

        return paymentClient
                .method(HttpMethod.PUT)
                .uri(URI_UPDATE_TRANSACTIONS_STATUS)
                .bodyValue(request)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                        response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new IllegalStateException(
                                        "Payment status update failed: httpStatus=%s, body=%s"
                                                .formatted(response.statusCode().value(), body)
                                ))))
                .bodyToMono(Integer.class)
                .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                        .filter(ex -> {
                            boolean retry = ex instanceof org.springframework.web.reactive.function.client.WebClientRequestException wcre
                                    && wcre.getCause() instanceof java.net.ConnectException;
                            if (retry) {
                                log.info("[PAYMENT_INTEGRATION] Retrying invocation due to exception: {}: {}",
                                        ex.getClass().getSimpleName(), ex.getMessage());
                            }
                            return retry;
                        }));
    }
}

