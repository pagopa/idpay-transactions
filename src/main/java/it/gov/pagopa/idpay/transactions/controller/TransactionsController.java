package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;


/**
 * Component that exposes APIs to find {@link RewardTransaction}
 * */
@RequestMapping("/idpay/transactions")
public interface TransactionsController {
    /**
     * Returns a set of transactions when trxDateStart and trxDateEnd not be null and one of the followed cases happens:
     * <ul>
     *     <li>The field idTrxIssuer is present</li>
     *     <li>The follows fields are present: userId and amount</li>
     * </ul>
     * */
    @GetMapping
    ResponseEntity<Flux<RewardTransaction>> findAll(
            @RequestParam(value = "idTrxIssuer", required = false) String idTrxIssuer,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "amount", required = false) BigDecimal amount,
            @RequestParam(value = "trxDateStart", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime trxDateStart,
            @RequestParam(value = "trxDateEnd", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime trxDateEnd
    );
}
