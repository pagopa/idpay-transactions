package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.common.reactive.utils.PerformanceLogger;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import java.time.Duration;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
@Slf4j
@CacheConfig
public class MerchantRestClientImpl implements MerchantRestClient {

  private static final String URI_POS_DETAIL = "/idpay/merchant/portal/{merchantId}/point-of-sales/{pointOfSaleId}";
  private final WebClient webClient;
  private final int retryDelay;
  private final long maxAttempts;

  public MerchantRestClientImpl(
      @Value("${app.merchant.base-url}") String baseUrl,
      @Value("${app.merchant.retry.delay-millis}") int retryDelay,
      @Value("${app.merchant.retry.max-attempts}") long maxAttempts,
      WebClient.Builder webClientBuilder
  ) {
    this.retryDelay = retryDelay;
    this.maxAttempts = maxAttempts;
    this.webClient = webClientBuilder.clone()
        .baseUrl(baseUrl)
        .build();
  }


  @Override
  @Cacheable(key = "#pointOfSaleId", value = "getPointOfSale", unless = "#result == null")
  public Mono<PointOfSaleDTO> getPointOfSale(String merchantId, String pointOfSaleId) {
    log.info("Sending request to merchant {} to get pos {}", merchantId, pointOfSaleId);
    return PerformanceLogger.logTimingOnNext(
            "MERCHANT_INTEGRATION",
            webClient
                .method(HttpMethod.GET)
                .uri(URI_POS_DETAIL,
                    Map.of("merchantId", merchantId,
                        "pointOfSaleId", pointOfSaleId))
                .retrieve()
                .toEntity(PointOfSaleDTO.class),
            x -> "httpStatus %s".formatted(x.getStatusCode().value())
        )
        .map(HttpEntity::getBody)

        .retryWhen(Retry.fixedDelay(maxAttempts, Duration.ofMillis(retryDelay))
            .filter(ex -> {
              boolean retry =
                  (ex instanceof WebClientResponseException.TooManyRequests) ||
                      ex.getMessage().startsWith("Connection refused");

              if (retry) {
                log.info("[MERCHANT_INTEGRATION] Retrying invocation due to exception: {}: {}",
                    ex.getClass().getSimpleName(), ex.getMessage());
              }

              return retry;
            })
        )

        .onErrorResume(WebClientResponseException.NotFound.class, ex -> {
          log.warn("POS {} not found for merchant {}", pointOfSaleId, merchantId);
          return Mono.empty();
        })
        .onErrorResume(WebClientResponseException.BadRequest.class, ex -> {
          log.warn("Invalid POS request for merchant {}", merchantId);
          return Mono.empty();
        });
  }
}
