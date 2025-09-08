package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@Slf4j
public class PointOfSaleTransactionControllerImpl implements PointOfSaleTransactionController {

  private final PointOfSaleTransactionService pointOfSaleTransactionService;
  private final PointOfSaleTransactionMapper mapper;

  public PointOfSaleTransactionControllerImpl(PointOfSaleTransactionService pointOfSaleTransactionService,
      PointOfSaleTransactionMapper mapper) {
    this.pointOfSaleTransactionService = pointOfSaleTransactionService;
    this.mapper = mapper;
  }

  @Override
  public Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String fiscalCode, String status, Pageable pageable) {
    log.info("[GET_POINT-OF-SALE_TRANSACTIONS] Point Of Sale {} requested to retrieve transactions", pointOfSaleId);

    Sort.Order fiscalCodeOrder = pageable.getSort().getOrderFor("fiscalCode");

    return pointOfSaleTransactionService.getPointOfSaleTransactions(merchantId, initiativeId, pointOfSaleId, fiscalCode, status, pageable)
        .flatMap(tuple ->
            Flux.fromIterable(tuple.getT1())
                .flatMapSequential(trx -> mapper.toDTO(trx, initiativeId, fiscalCode))
                .collectList()
                .map(dtoList -> {
                      if (fiscalCodeOrder != null) {
                        boolean asc = fiscalCodeOrder.isAscending();
                        dtoList.sort((a, b) -> {
                          int cmp = a.getFiscalCode().compareToIgnoreCase(b.getFiscalCode());
                          return asc ? cmp : -cmp;
                        });

                      int pageNo = pageable.getPageNumber();
                      int pageSize = pageable.getPageSize();
                      int start = pageNo * pageSize;
                      int end = Math.min(start + pageSize, dtoList.size());
                      dtoList = start < end ? dtoList.subList(start, end) : List.of();
                    }

                  Page<PointOfSaleTransactionDTO> page = new PageImpl<>(dtoList, pageable, tuple.getT2());

                  return new PointOfSaleTransactionsListDTO(
                      dtoList,
                      page.getNumber(),
                      page.getSize(),
                      (int) page.getTotalElements(),
                      page.getTotalPages());
                })
        );
  }
}
