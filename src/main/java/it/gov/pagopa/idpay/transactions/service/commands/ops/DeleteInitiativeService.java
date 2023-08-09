package it.gov.pagopa.idpay.transactions.service.commands.ops;

import reactor.core.publisher.Mono;

public interface DeleteInitiativeService {
    Mono<String> execute(String initiativeId);
}
