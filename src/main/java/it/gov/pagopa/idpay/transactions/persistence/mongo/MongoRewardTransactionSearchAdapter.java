package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSearchPort;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardTransactionSearchAdapter implements RewardTransactionSearchPort {

    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Flux<RewardTransaction> findMerchantTransactions(
            TrxFiltersDTO filters,
            String userId,
            boolean includeToCheckWithConsultable,
            Pageable pageable
    ) {
        return rewardTransactionRepository.findByFilter(
                filters,
                userId,
                includeToCheckWithConsultable,
                pageable
        );
    }

    @Override
    public Mono<Long> countMerchantTransactions(
            TrxFiltersDTO filters,
            String userId,
            boolean includeToCheckWithConsultable
    ) {
        return rewardTransactionRepository.getCount(
                filters,
                filters.getPointOfSaleId(),
                null,
                userId,
                includeToCheckWithConsultable
        );
    }

    @Override
    public Flux<RewardTransaction> findPointOfSaleTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String userId,
            String productGtin,
            boolean includeToCheckWithConsultable,
            Pageable pageable
    ) {
        return rewardTransactionRepository.findByFilterTrx(
                filters,
                pointOfSaleId,
                userId,
                productGtin,
                includeToCheckWithConsultable,
                pageable
        );
    }

    @Override
    public Mono<Long> countPointOfSaleTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String productGtin,
            String userId,
            boolean includeToCheckWithConsultable
    ) {
        return rewardTransactionRepository.getCount(
                filters,
                pointOfSaleId,
                productGtin,
                userId,
                includeToCheckWithConsultable
        );
    }
}
