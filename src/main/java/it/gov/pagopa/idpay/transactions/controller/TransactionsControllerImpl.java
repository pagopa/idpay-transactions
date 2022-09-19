package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@Slf4j
public class TransactionsControllerImpl implements TransactionsController{
    private final RewardTransactionService rewardTransactionService;

    public TransactionsControllerImpl(RewardTransactionService rewardTransactionService) {
        this.rewardTransactionService = rewardTransactionService;
    }

    @Override
    public ResponseEntity<Flux<?>> findAll(String idTrxAcquirer, String userId, String trxDate, String amount) {
        return ResponseEntity.ok(rewardTransactionService.findTrxsFilters(idTrxAcquirer, userId, trxDate, amount));
    }
}
