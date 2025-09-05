package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

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
  public Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String fiscalCode, String status, Pageable pageable) {

    return getPointOfSaleTransactionDTOs2Count(merchantId, initiativeId, pointOfSaleId, fiscalCode, status, pageable)
        .map(tuple -> {
          Page<PointOfSaleTransactionDTO> page = new PageImpl<>(tuple.getT1(),
              pageable, tuple.getT2());
          return new PointOfSaleTransactionsListDTO(tuple.getT1(), page.getNumber(), page.getSize(),
              (int) page.getTotalElements(), page.getTotalPages());
        });
  }

  private Mono<Tuple2<List<PointOfSaleTransactionDTO>, Long>> getPointOfSaleTransactionDTOs2Count(String merchantId, String initiativeId, String pointOfSaleId, String fiscalCode, String status, Pageable pageable) {
    if (StringUtils.isNotBlank(fiscalCode)){
      return Mono.just(fiscalCode)
          .flatMap(userRestClient::retrieveFiscalCodeInfo)
          .map(FiscalCodeInfoPDV::getToken)
          .flatMap(userId -> getPointOfSaleTransactionDTOs(merchantId, initiativeId, pointOfSaleId, status, pageable, userId, fiscalCode));
    } else {
      return getPointOfSaleTransactionDTOs(merchantId, initiativeId, pointOfSaleId, status, pageable, null, fiscalCode);
    }
  }

  private Mono<Tuple2<List<PointOfSaleTransactionDTO>, Long>> getPointOfSaleTransactionDTOs(String merchantId, String initiativeId, String pointOfSaleId, String status, Pageable pageable, String userId, String fiscalCode) {
    boolean isFiscalCodeSort = false;
    Pageable effectivePageable = pageable;

    if (pageable != null) {
      if (pageable.getSort().getOrderFor("fiscalCode") != null) {
        isFiscalCodeSort = true;
      } else if (pageable.getSort().getOrderFor("userId") != null) {
        Sort.Order order = pageable.getSort().getOrderFor("userId");
        Sort forcedSort = Sort.by(Objects.requireNonNull(order).isAscending()
            ? Sort.Direction.ASC
            : Sort.Direction.DESC,
            "fiscalCode");
        effectivePageable = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), forcedSort);
        isFiscalCodeSort = true;
      }
    }

    final boolean sortByFiscalCode = isFiscalCodeSort;
    final Pageable finalPageable = effectivePageable;

    Pageable repoPageable = sortByFiscalCode ? null : finalPageable;

    return rewardTransactionRepository.findByFilter(merchantId, initiativeId, pointOfSaleId, userId, status, repoPageable)
        .concatMap(t -> createPointOfSaleTransactionDTO(initiativeId, t, fiscalCode))
        .collectList()
        .zipWith(rewardTransactionRepository.getCount(merchantId, initiativeId, pointOfSaleId, userId, status))
        .map(tuple -> {
          List<PointOfSaleTransactionDTO> list = tuple.getT1();
          long totalElements = tuple.getT2();

          if (sortByFiscalCode) {
            boolean asc = Objects.requireNonNull(finalPageable.getSort().getOrderFor("fiscalCode")).isAscending();

            list.sort((a, b) -> {
              int cmp = a.getFiscalCode().compareToIgnoreCase(b.getFiscalCode());
              return asc ? cmp : -cmp;
            });

            int pageNumber = finalPageable.getPageNumber();
            int pageSize = finalPageable.getPageSize();
            int fromIndex = Math.min(pageNumber * pageSize, list.size());
            int toIndex = Math.min(fromIndex + pageSize, list.size());
            list = list.subList(fromIndex, toIndex);
          }

          return Tuples.of(list, totalElements);
        });
  }

  private Mono<PointOfSaleTransactionDTO> createPointOfSaleTransactionDTO(String initiativeId, RewardTransaction transaction, String fiscalCode) {
    PointOfSaleTransactionDTO out = PointOfSaleTransactionDTO.builder()
        .trxId(transaction.getId())
        .effectiveAmountCents(transaction.getAmountCents())
        .rewardAmountCents(transaction.getRewards().get(initiativeId).getAccruedRewardCents())
        .trxDate(transaction.getTrxDate())
        .elaborationDateTime(transaction.getElaborationDateTime())
        .status(transaction.getStatus())
        .channel(transaction.getChannel())
        .build();

    if (StringUtils.isNotBlank(fiscalCode)){
      out.setFiscalCode(fiscalCode);
      return Mono.just(out);
    } else {
      return userRestClient.retrieveUserInfo(transaction.getUserId())
          .map(UserInfoPDV::getPii)
          .doOnNext(out::setFiscalCode)
          .then(Mono.just(out));
    }
  }
}
