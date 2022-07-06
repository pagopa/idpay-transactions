package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface RewardTransactionRepository extends ReactiveMongoRepository<RewardTransaction,String> {
    Mono<RewardTransactionDTO> findByIdTrxAcquirerAndAcquirerCodeAndTrxDateAndOperationTypeAndAcquirerId(String idTrxAcquirer,
                                                                                                         String acquirerCode,
                                                                                                         String trxDate,
                                                                                                         String operationType,
                                                                                                         String acquirerId);
}
