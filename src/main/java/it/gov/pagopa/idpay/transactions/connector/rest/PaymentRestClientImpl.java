package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

@Service
@Slf4j
public class PaymentRestClientImpl implements PaymentRestClient {

    private static final String URI_CANCEL_TRANSACTION = "/idpay/payment/transactions/{transactionId}";

    private final WebClient webClient;
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
        this.webClient = webClientBuilder.clone()
                .baseUrl(baseUrl)
                .build();
    }

    @Override
    public Mono<Void> cancelTransaction(String transactionId, String merchantId, String acquirerId, String pointOfSaleId) {
        log.info("Sending cancel transaction request for transactionId {}", transactionId);

        return PerformanceLogger.logTimingOnNext(
                        "PAYMENT_INTEGRATION",
                        webClient
                                .method(HttpMethod.DELETE)
                                .uri(URI_CANCEL_TRANSACTION, Map.of("transactionId", transactionId))
                                .header("x-merchant-id", merchantId)
                                .header("x-acquirer-id", acquirerId)
                                .header("x-point-of-sale-id", pointOfSaleId)
                                .retrieve()
                                .toBodilessEntity(),
                        x -> "httpStatus %s".formatted(x.getStatusCode().value())
                )
                .then()
                .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
                        .filter(ex -> {
                            boolean retry =
                                    (ex instanceof WebClientResponseException.TooManyRequests) ||
                                            ex.getMessage().startsWith("Connection refused");

                            if (retry) {
                                log.info("[PAYMENT_INTEGRATION] Retrying invocation due to exception: {}: {}",
                                        ex.getClass().getSimpleName(), ex.getMessage());
                            }

                            return retry;
                        })
                )
                .onErrorResume(WebClientResponseException.NotFound.class, ex -> {
                    log.warn("Transaction {} not found on payment service", transactionId);
                    return Mono.empty();
                })
                .onErrorResume(WebClientResponseException.BadRequest.class, ex -> {
                    log.warn("Invalid cancel transaction request for transactionId {}", transactionId);
                    return Mono.empty();
                });
    }
}

