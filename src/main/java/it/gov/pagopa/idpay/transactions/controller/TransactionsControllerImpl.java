package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@Slf4j
public class TransactionsControllerImpl implements TransactionsController{
    private final RewardTransactionService rewardTransactionService;

    public TransactionsControllerImpl(RewardTransactionService rewardTransactionService) {
        this.rewardTransactionService = rewardTransactionService;
    }


    @Override
    public ResponseEntity<String> cleanupInvoicedTransactions(Integer chunkSize, Integer repetitionsNumber, boolean processAll, String trxId) {
        log.info("[BATCH_ASSIGNMENT] Start processing INVOICED transactions without batch");
        String  jobId  = UUID.randomUUID().toString();
        rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize,  repetitionsNumber, processAll,  trxId)
                .doOnSubscribe(sub  -> log.info("[BATCH_ASSIGNMENT]  Job  {}  started", jobId))
                .doOnError(err  ->  log.error("[BATCH_ASSIGNMENT] Job  {}  failed:  {}", jobId,  err.getMessage()))
                .doOnSuccess(v  -> log.info("[BATCH_ASSIGNMENT]  Job  {}  completed", jobId))
                .subscribe();
        return  ResponseEntity.accepted().body(jobId);
    }



}