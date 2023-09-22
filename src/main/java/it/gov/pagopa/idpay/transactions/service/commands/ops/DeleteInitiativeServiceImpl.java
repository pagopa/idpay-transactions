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
        return deleteTransactions(payload.getEntityId(), Integer.parseInt(payload.getAdditionalParams().get(PAGINATION_KEY)), Long.parseLong(payload.getAdditionalParams().get(DELAY_KEY)))
                .then(Mono.just(payload.getEntityId()));
    }

    private Mono<Void> deleteTransactions(String initiativeId, int pageSize, long delayDuration){
        return rewardTransactionRepository.findOneByInitiativeId(initiativeId)
                .flatMap(trx -> {
                    if ("QRCODE".equals(trx.getChannel())){
                        return rewardTransactionRepository.deleteByInitiativeIdPaged(initiativeId, pageSize)
                                .delayElement(Duration.ofMillis(delayDuration))
                                .expand(deletedPage -> {
                                    if(deletedPage.getDeletedCount() < pageSize){
                                        return Mono.empty();
                                    } else {
                                        return rewardTransactionRepository.deleteByInitiativeIdPaged(initiativeId, pageSize)
                                                .delayElement(Duration.ofMillis(delayDuration));
                                    }
                                })
                                .reduce((long)0, (totalDeletedElements, deletedPage) -> totalDeletedElements + deletedPage.getDeletedCount())
                                .doOnNext(totalDeletedElements -> {
                                    log.info("[DELETE_INITIATIVE] Deleted initiative {} from collection: transaction", initiativeId);
                                    auditUtilities.logTransactionsDeleted((totalDeletedElements), initiativeId);
                                })
                                .then();
                    } else {
                        return rewardTransactionRepository.findAndRemoveInitiativeOnTransactionPaged(initiativeId, pageSize)
                                .delayElement(Duration.ofMillis(delayDuration))
                                .expand(deletedPage -> {
                                    if(deletedPage.getModifiedCount() < pageSize){
                                        return Mono.empty();
                                    } else {
                                        return rewardTransactionRepository.findAndRemoveInitiativeOnTransactionPaged(initiativeId, pageSize)
                                                .delayElement(Duration.ofMillis(delayDuration));
                                    }
                                })
                                .reduce((long)0, (totalUpdatedElements, updatedPage) -> totalUpdatedElements + updatedPage.getModifiedCount())
                                .doOnNext(totalUpdatedElements -> {
                                    log.info("[DELETE_INITIATIVE] Deleted initiative {} from collection: transaction", initiativeId);
                                    auditUtilities.logTransactionsDeleted(totalUpdatedElements, initiativeId);
                                })
                                .then();
                    }
                });
    }

    /*
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

     */
}
