package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.time.LocalDateTime;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardTransactionSearchPort {

    Flux<RewardTransaction> findMerchantTransactions(
            TrxFiltersDTO filters,
            String userId,
            boolean includeToCheckWithConsultable,
            Pageable pageable
    );

    Mono<Long> countMerchantTransactions(
            TrxFiltersDTO filters,
            String userId,
            boolean includeToCheckWithConsultable
    );

    Flux<RewardTransaction> findPointOfSaleTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String userId,
            String productGtin,
            boolean includeToCheckWithConsultable,
            Pageable pageable
    );

    Mono<Long> countPointOfSaleTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String productGtin,
            String userId,
            boolean includeToCheckWithConsultable
    );

    Flux<RewardTransaction> findByIdTrxIssuer(
            String idTrxIssuer,
            String userId,
            LocalDateTime trxDateStart,
            LocalDateTime trxDateEnd,
            Long amountCents,
            Pageable pageable
    );

    Flux<RewardTransaction> findByRange(
            String userId,
            LocalDateTime trxDateStart,
            LocalDateTime trxDateEnd,
            Long amountCents,
            Pageable pageable
    );

    Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId);
}
