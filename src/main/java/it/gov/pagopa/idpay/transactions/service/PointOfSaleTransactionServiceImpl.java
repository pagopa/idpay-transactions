package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

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
  public Mono<Tuple2<List<RewardTransaction>, Long>> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String fiscalCode, String status, Pageable pageable) {
    if (StringUtils.isNotBlank(fiscalCode)) {
      return userRestClient.retrieveFiscalCodeInfo(fiscalCode)
          .map(FiscalCodeInfoPDV::getToken)
          .flatMap(userId ->
             getTransactions(merchantId, initiativeId, pointOfSaleId, userId, status, pageable));
    } else {
      return getTransactions(merchantId, initiativeId, pointOfSaleId, null, status, pageable);
    }
  }

  private Mono<Tuple2<List<RewardTransaction>, Long>> getTransactions(String merchantId, String initiativeId, String pointOfSaleId, String userId, String status, Pageable pageable) {
    Pageable repoPageable = isSortOnFiscalCode(pageable) ? null : pageable;

    return rewardTransactionRepository.findByFilterTrx(merchantId, initiativeId, pointOfSaleId, userId, status, repoPageable)
          .collectList()
          .zipWith(rewardTransactionRepository.getCount(merchantId, initiativeId, pointOfSaleId, userId, status));
  }

  private boolean isSortOnFiscalCode(Pageable pageable) {
    return pageable != null && pageable.getSort().getOrderFor("fiscalCode") != null;
  }
}
