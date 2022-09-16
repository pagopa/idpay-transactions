package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class TransactionsControllerImpl implements TransactionsController{
    private final RewardTransactionService rewardTransactionService;

    public TransactionsControllerImpl(RewardTransactionService rewardTransactionService) {
        this.rewardTransactionService = rewardTransactionService;
    }

    @Override
    public ResponseEntity<Flux<?>> findAll(String startDate, String endDate, String userId, String hpan, String acquirerId) {
    Flux<RewardTransaction> retrieved = rewardTransactionService.findAll(startDate, endDate, userId, hpan, acquirerId);
        if(retrieved==null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Flux.just("The following filters are mandatory: startDate, endDate"));
        } else{
            return ResponseEntity.ok().body(retrieved);
        }
    }
}
