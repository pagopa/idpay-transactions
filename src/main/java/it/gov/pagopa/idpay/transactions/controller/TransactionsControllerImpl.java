package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@RestController
@Slf4j
public class TransactionsControllerImpl implements TransactionsController{
    private final RewardTransactionService rewardTransactionService;

    public TransactionsControllerImpl(RewardTransactionService rewardTransactionService) {
        this.rewardTransactionService = rewardTransactionService;
    }

    @Override
    public ResponseEntity<Flux<RewardTransaction>> findAll(String idTrxAcquirer, String userId, BigDecimal amount, LocalDateTime trxDateStart, LocalDateTime trxDateEnd) {
        if (trxDateStart == null || trxDateEnd == null) {
            throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","The mandatory filters are: trxDateStart, trxDateEnd, and one of the following options: 1) idTrxIssuer 2) userId and amount");
        } else {
            return ResponseEntity.ok(rewardTransactionService.findTrxsFilters(idTrxAcquirer, userId, amount, trxDateStart, trxDateEnd));
        }
    }
}
