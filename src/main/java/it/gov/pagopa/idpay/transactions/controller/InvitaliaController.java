package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/invitalia")
public interface InvitaliaController {
    @GetMapping("/token")
    Mono<String> getToken();

    @PostMapping("/erogazioni")
    Mono<Void> postErogazione(
            @RequestBody DeliveryRequest deliveryRequest
    );

    @PatchMapping("/checkRefundOutcome")
    Mono<Void> checkRefundOutcome(
            @RequestParam String rewardBatchId
    );
}
