package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;


/**
 * Component that exposes APIs to find {@link RewardTransaction}
 * */
@RequestMapping("/idpay/transactions")
public interface TransactionsController {


    @PostMapping("/cleanup")
    ResponseEntity<String> cleanupInvoicedTransactions(
            @RequestParam(defaultValue = "200") Integer chunkSize,
            @RequestParam(defaultValue = "1") Integer repetitionsNumber,
            @RequestParam(defaultValue = "false") boolean processAll,
            @RequestParam(required = false) String trxId
    );


}