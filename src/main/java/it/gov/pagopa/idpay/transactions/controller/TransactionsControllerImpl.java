package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;

@RestController
@Slf4j
public class TransactionsControllerImpl implements TransactionsController{
    private final RewardTransactionService rewardTransactionService;

    public TransactionsControllerImpl(RewardTransactionService rewardTransactionService) {
        this.rewardTransactionService = rewardTransactionService;
    }

    @Override
    public Flux<RewardTransaction> findAll(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        if(idTrxIssuer != null){
            return rewardTransactionService.findByIdTrxIssuer(idTrxIssuer,userId,trxDateStart, trxDateEnd, amountCents, pageable);
        }else if(userId != null && trxDateStart != null && trxDateEnd != null){
            return rewardTransactionService.findByRange(userId, trxDateStart, trxDateEnd, amountCents, pageable);
        }else {
            throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS,ExceptionConstants.ExceptionMessage.TRANSACTIONS_MISSING_MANDATORY_FILTERS);
        }
    }
}