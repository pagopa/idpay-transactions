package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.enums.BatchType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.time.LocalDate;
import java.time.YearMonth;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@Slf4j
public class RewardTransactionServiceImpl implements RewardTransactionService {

    private final RewardTransactionRepository rewardTrxRepository;
    private final RewardBatchService rewardBatchService;
    private final MerchantRestClient merchantRestClient;


    public RewardTransactionServiceImpl(RewardTransactionRepository rewardTrxRepository,
        RewardBatchService rewardBatchService, MerchantRestClient merchantRestClient) {
        this.rewardTrxRepository = rewardTrxRepository;
        this.rewardBatchService = rewardBatchService;
      this.merchantRestClient = merchantRestClient;
    }

    @Override
    public Mono<RewardTransaction> save(RewardTransaction rewardTransaction) {

        if (SyncTrxStatus.INVOICED.name().equals(rewardTransaction.getStatus())) {
            return enrichBatchData(rewardTransaction)
                .flatMap(rewardTrxRepository::save);
        }
        return rewardTrxRepository.save(rewardTransaction);
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        return rewardTrxRepository.findByIdTrxIssuer(idTrxIssuer, userId, trxDateStart, trxDateEnd, amountCents, pageable);
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable) {
        return rewardTrxRepository.findByRange(userId, trxDateStart, trxDateEnd, amountCents, pageable);
    }

    @Override
    public Mono<RewardTransaction> findByTrxIdAndUserId(String trxId, String userId){
        return rewardTrxRepository.findByTrxIdAndUserId(trxId, userId);
    }

    private Mono<RewardTransaction> enrichBatchData(RewardTransaction trx) {

        LocalDate trxDate = trx.getTrxChargeDate().toLocalDate();
        YearMonth trxMonth = YearMonth.from(trxDate);

        String batchMonth = trxMonth.plusMonths(1).toString();

        return merchantRestClient.getPointOfSale(trx.getMerchantId(), trx.getPointOfSaleId())
            .map(pos -> PosType.valueOf(pos.getType().name()))
            .flatMap(posType ->
                rewardBatchService.findOrCreateBatch(
                    trx.getMerchantId(),
                    posType,
                    batchMonth,
                    BatchType.REGULAR
                )
            )
            .map((RewardBatch batch) -> {
                trx.setRewardBatchId(batch.getId());
                trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
                trx.setRewardBatchInclusionDate(LocalDateTime.now());
                trx.setRewardBatchRejectionReason(null);
                return trx;
            });
    }
}
