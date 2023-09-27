package it.gov.pagopa.idpay.transactions.service.commands.ops;

import it.gov.pagopa.idpay.transactions.dto.QueueCommandOperationDTO;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@Slf4j
public class DeleteInitiativeServiceImpl implements DeleteInitiativeService{
    private final RewardTransactionRepository rewardTransactionRepository;
    private final AuditUtilities auditUtilities;
    private static final String PAGINATION_KEY = "pagination";
    private static final String DELAY_KEY = "delay";

    @SuppressWarnings("squid:S00107") // suppressing too many parameters constructor alert
    public DeleteInitiativeServiceImpl(RewardTransactionRepository rewardTransactionRepository,
                                       AuditUtilities auditUtilities) {
        this.rewardTransactionRepository = rewardTransactionRepository;
        this.auditUtilities = auditUtilities;
    }

    @Override
    public Mono<String> execute(QueueCommandOperationDTO payload) {
        log.info("[DELETE_INITIATIVE] Starting handle delete initiative {}", payload.getEntityId());
        return deleteTransactions(payload.getEntityId(),
                Integer.parseInt(payload.getAdditionalParams().get(PAGINATION_KEY)),
                Long.parseLong(payload.getAdditionalParams().get(DELAY_KEY)))
                .then(Mono.just(payload.getEntityId()));
    }

    private Mono<Void> deleteTransactions(String initiativeId, int pageSize, long delayDuration){
        Mono<Long> monoDelay = Mono.delay(Duration.ofMillis(delayDuration));

        return rewardTransactionRepository.findOneByInitiativeId(initiativeId)
                .flatMap(trx -> {
                    if ("QRCODE".equals(trx.getChannel())){
                        return rewardTransactionRepository.findByInitiativesWithBatch(initiativeId, pageSize)
                                .flatMap(trxToDelete -> rewardTransactionRepository.deleteById(trxToDelete.getId())
                                        .then(monoDelay), pageSize)
                                .count()
                                .doOnNext(totalDeletedTrx -> {
                                    log.info("[DELETE_INITIATIVE] Deleted initiative {} from collection: transaction", initiativeId);
                                    auditUtilities.logTransactionsDeleted((totalDeletedTrx), initiativeId);
                                })
                                .then();
                    } else {
                        return rewardTransactionRepository.findByInitiativesWithBatch(initiativeId, pageSize)
                                .flatMap(trxToUpdate -> rewardTransactionRepository.removeInitiativeOnTransaction(trxToUpdate.getId(), initiativeId)
                                        .then(monoDelay), pageSize)
                                .count()
                                .doOnNext(totalUpdatedTrx -> {
                                    log.info("[DELETE_INITIATIVE] Deleted initiative {} from collection: transaction", initiativeId);
                                    auditUtilities.logTransactionsDeleted((totalUpdatedTrx), initiativeId);
                                })
                                .then();
                    }
                });
    }
}
