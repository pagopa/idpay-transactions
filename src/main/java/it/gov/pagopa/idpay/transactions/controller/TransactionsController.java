package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Flux;



/**
 * Component that exposes APIs to find {@link RewardTransaction}
 * */
@RequestMapping("/idpay/transactions")
public interface TransactionsController {
    /**
     * Returns a set of transactions when one of the followed cases happens:
     * <ul>
     *     <li>The field idTrxIssuer is present</li>
     *     <li>The follows fields are not null: userId, trxDate and amount</li>
     * </ul>
     * */
    @GetMapping
    ResponseEntity<Flux<RewardTransaction>> findAll(
            @RequestParam(value = "idTrxIssuer", required = false) String idTrxIssuer,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "trxDate", required = false) String trxDate,
            @RequestParam(value = "amount", required = false) String amount
    );
}
