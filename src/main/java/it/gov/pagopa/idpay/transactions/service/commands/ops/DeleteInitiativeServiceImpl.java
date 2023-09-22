package it.gov.pagopa.idpay.transactions.service.commands.ops;

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

    @SuppressWarnings("squid:S00107") // suppressing too many parameters constructor alert
    public DeleteInitiativeServiceImpl(RewardTransactionRepository rewardTransactionRepository,
                                       AuditUtilities auditUtilities) {
        this.rewardTransactionRepository = rewardTransactionRepository;
        this.auditUtilities = auditUtilities;
    }

    @Override
    public Mono<String> execute(String initiativeId) {
        log.info("[DELETE_INITIATIVE] Starting handle delete initiative {}", initiativeId);
        return delete(initiativeId, 100, 30000)
                .then(Mono.just(initiativeId));
    }

    /*
    @Override
    public Mono<String> execute(String initiativeId) {
        log.info("[DELETE_INITIATIVE] Starting handle delete initiative {}", initiativeId);
        return deleteTransactions(initiativeId)
                .then(Mono.just(initiativeId));
    }
     */

    private Mono<Void> deleteTransactions(String initiativeId){
        return rewardTransactionRepository.findOneByInitiativeId(initiativeId)
                .flatMap(trx -> {
                    if ("QRCODE".equals(trx.getChannel())){
                        return rewardTransactionRepository.deleteByInitiativeId(initiativeId)
                                .doOnNext(response -> {
                                    log.info("[DELETE_INITIATIVE] Deleted initiative {} from collection: transaction", initiativeId);
                                    auditUtilities.logTransactionsDeleted(response.getDeletedCount(), initiativeId);
                                })
                                .then();
                    } else {
                        return rewardTransactionRepository.findAndRemoveInitiativeOnTransaction(initiativeId)
                                .doOnNext(updateResult -> {
                                    log.info("[DELETE_INITIATIVE] Deleted initiative {} from collection: transaction", initiativeId);
                                    auditUtilities.logTransactionsDeleted(updateResult.getModifiedCount(), initiativeId);
                                })
                                .then();
                    }
                });
    }

    private Mono<Void> delete(String initiativeId,int pageSize, long duration){
        return rewardTransactionRepository.deletePaged(initiativeId, pageSize)
                .delayElement(Duration.ofMillis(duration))
                .expand(deletedPage -> {
                    if(deletedPage.getDeletedCount() < pageSize){
                        return Mono.empty();
                    } else {
                        return rewardTransactionRepository.deletePaged(initiativeId, pageSize)
                                .delayElement(Duration.ofMillis(duration));
                    }
                })
                .then();
    }
}
