package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchesRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/erogazioni")
public interface ErogazioniController {
    @PostMapping("/sendErogazione")
    Mono<Void> sendErogazione(
            @RequestBody DeliveryRequest deliveryRequest
    );
}
