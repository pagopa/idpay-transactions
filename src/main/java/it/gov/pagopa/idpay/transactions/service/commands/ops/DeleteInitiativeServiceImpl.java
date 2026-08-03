package it.gov.pagopa.idpay.transactions.service.commands.ops;

import it.gov.pagopa.idpay.transactions.persistence.port.InitiativeTransactionDeletionPort;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class DeleteInitiativeServiceImpl implements DeleteInitiativeService{
    private final InitiativeTransactionDeletionPort initiativeTransactionDeletionPort;
    private final AuditUtilities auditUtilities;

    public DeleteInitiativeServiceImpl(InitiativeTransactionDeletionPort initiativeTransactionDeletionPort,
                                       AuditUtilities auditUtilities) {
        this.initiativeTransactionDeletionPort = initiativeTransactionDeletionPort;
        this.auditUtilities = auditUtilities;
    }

    @Override
    public Mono<String> execute(String initiativeId) {
        log.info("[DELETE_INITIATIVE] Starting handle delete initiative {}", initiativeId);
        return initiativeTransactionDeletionPort.deleteTransactions(initiativeId)
                .doOnNext(totalDeletedTrx -> {
                    if (totalDeletedTrx > 0) {
                        log.info("[DELETE_INITIATIVE] Deleted initiative {} from table: reward_transactions", initiativeId);
                        auditUtilities.logTransactionsDeleted(totalDeletedTrx, initiativeId);
                    }
                })
                .thenReturn(initiativeId);
    }
}
