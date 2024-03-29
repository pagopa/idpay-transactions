package it.gov.pagopa.idpay.transactions.service.commands;

import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

public interface CommandsMediatorService {
    void execute(Flux<Message<String>> initiativeDTOFlux);
}
