package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
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

    return pointOfSaleTransactionService.getPointOfSaleTransactions(merchantId, initiativeId, pointOfSaleId, fiscalCode, status, pageable)
        .flatMap(page ->
            Flux.fromIterable(page.getContent())
                .flatMapSequential(trx -> mapper.toDTO(trx, initiativeId, fiscalCode))
                .collectList()
                .map(dtoList -> new PointOfSaleTransactionsListDTO(
                    dtoList,
                    page.getNumber(),
                    page.getSize(),
                    (int) page.getTotalElements(),
                    page.getTotalPages()))
        );
  }
}
