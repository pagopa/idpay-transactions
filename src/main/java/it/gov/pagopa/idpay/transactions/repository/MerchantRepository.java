package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.Merchant;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface MerchantRepository extends ReactiveMongoRepository<Merchant,String> {
}
