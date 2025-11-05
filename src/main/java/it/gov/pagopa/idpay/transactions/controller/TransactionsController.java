package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;


/**
 * Component that exposes APIs to find {@link RewardTransaction}
 * */
@RequestMapping("/idpay/transactions")
public interface TransactionsController {
    /**
     * Returns a set of transactions when one of the followed cases happens:
     * <ul>
     *     <li>The field idTrxIssuer is present</li>
     *     <li>The follows fields are present: userId, trxDateStart and trxDateEnd</li>
     * </ul>
     * */
    @GetMapping
    Flux<RewardTransaction> findAll(
            @RequestParam(value = "idTrxIssuer", required = false) String idTrxIssuer,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "trxDateStart", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime trxDateStart,
            @RequestParam(value = "trxDateEnd", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime trxDateEnd,
            @RequestParam(value = "amountCents", required = false) Long amountCents,
            @PageableDefault(size = 2000) Pageable pageable
    );

    @GetMapping("/{trxId}/{userId}")
    Mono<RewardTransaction> findByTrxIdAndUserId(
            @PathVariable(value = "trxId") String trxId,
            @PathVariable(value = "userId") String userId
    );
}
