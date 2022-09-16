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
     * Returns the actual a transaction
     *
     * with mandatory filters
     * <ul>
     *     <li>startDate</li>
     *     <li>endDate</li>
     * </ul>
     * and optionally filters
     * <ul>
     *     <li>userId</li>
     *     <li>hpan</li>
     *     <li>acquirerId</li>
     * </ul>
     */
    @GetMapping
    ResponseEntity<Flux<?>> findAll(
            @RequestParam(value = "startDate",required = false) String startDate,
            @RequestParam(value = "endDate", required = false) String endDate,
            @RequestParam(value = "userId", required = false)  String userId,
            @RequestParam(value = "hpan", required = false)  String hpan,
            @RequestParam(value = "acquirerId", required = false)  String acquirerId
    );
}
