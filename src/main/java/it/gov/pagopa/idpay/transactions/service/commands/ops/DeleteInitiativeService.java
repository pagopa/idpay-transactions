package it.gov.pagopa.idpay.transactions.service.commands.ops;

import it.gov.pagopa.idpay.transactions.dto.QueueCommandOperationDTO;
import reactor.core.publisher.Mono;

public interface DeleteInitiativeService {
    Mono<String> execute(QueueCommandOperationDTO payload);
}
