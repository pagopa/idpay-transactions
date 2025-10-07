package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class PointOfSaleTransactionServiceImpl implements PointOfSaleTransactionService {

  private final UserRestClient userRestClient;
  private final RewardTransactionRepository rewardTransactionRepository;

  protected PointOfSaleTransactionServiceImpl(
      UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository) {
    this.userRestClient = userRestClient;
    this.rewardTransactionRepository = rewardTransactionRepository;
  }

  @Override
  public Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String productGtin, String fiscalCode, String status, Pageable pageable) {
    if (StringUtils.isNotBlank(fiscalCode)) {
      return userRestClient.retrieveFiscalCodeInfo(fiscalCode)
          .map(FiscalCodeInfoPDV::getToken)
          .flatMap(userId ->
              getTransactions(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status, pageable));
    } else {
      return getTransactions(merchantId, initiativeId, pointOfSaleId, null, productGtin, status, pageable);
    }
  }

  private Mono<Page<RewardTransaction>> getTransactions(String merchantId, String initiativeId, String pointOfSaleId, String userId, String productGtin, String status, Pageable pageable) {
    return rewardTransactionRepository.findByFilterTrx(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status, pageable)
        .collectList()
        .zipWith(rewardTransactionRepository.getCount(merchantId, initiativeId, pointOfSaleId, userId, productGtin, status))
        .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
  }
}
