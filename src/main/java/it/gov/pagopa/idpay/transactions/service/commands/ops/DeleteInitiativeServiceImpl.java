package it.gov.pagopa.idpay.transactions.service.commands.ops;

import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@Slf4j
public class DeleteInitiativeServiceImpl implements DeleteInitiativeService{
    private final RewardTransactionRepository rewardTransactionRepository;
    private final AuditUtilities auditUtilities;
    private final int pageSize;
    private final Mono<Long> monoDelay;

    @SuppressWarnings("squid:S00107") // suppressing too many parameters constructor alert
    public DeleteInitiativeServiceImpl(RewardTransactionRepository rewardTransactionRepository,
                                       AuditUtilities auditUtilities,
                                       @Value("${app.delete.paginationSize}")int pageSize,
                                       @Value("${app.delete.delayTime}")long delay) {
        this.rewardTransactionRepository = rewardTransactionRepository;
        this.auditUtilities = auditUtilities;
        this.pageSize = pageSize;
        this.monoDelay = Mono.delay(Duration.ofMillis(delay));
    }

    @Override
    public Mono<String> execute(String initiativeId) {
        log.info("[DELETE_INITIATIVE] Starting handle delete initiative {}", initiativeId);
        return deleteTransactions(initiativeId)
                .then(Mono.just(initiativeId));
    }

    private Mono<Void> deleteTransactions(String initiativeId){
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
