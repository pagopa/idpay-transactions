package it.gov.pagopa.idpay.transactions.service.commands.ops;

import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

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
        return deletedTransactions(initiativeId)
                .then(Mono.just(initiativeId));
    }

    private Mono<Void> deletedTransactions(String initiativeId){
        return rewardTransactionRepository.findOneByInitiativeId(initiativeId)
                .flatMap(trx -> {
                    if ("QRCODE".equals(trx.getChannel())){
                        return rewardTransactionRepository.deleteByInitiativeId(initiativeId).count()
                                .doOnNext(count -> auditUtilities.logTransactionsDeleted(count, initiativeId))
                                .then();
                    } else {
                        return rewardTransactionRepository.findAndRemoveInitiativeOnTransaction(initiativeId)
                                .doOnNext(updateResult -> auditUtilities.logTransactionsDeleted(updateResult.getModifiedCount(), initiativeId))
                                .then();
                    }
                });
    }
}
