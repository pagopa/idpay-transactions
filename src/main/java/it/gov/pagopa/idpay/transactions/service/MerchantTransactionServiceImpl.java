package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.Comparator;
import java.util.List;

@Service
public class MerchantTransactionServiceImpl implements MerchantTransactionService {
    private final UserRestClient userRestClient;
    private final RewardTransactionRepository rewardTransactionRepository;

    protected MerchantTransactionServiceImpl(
            UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository) {
        this.userRestClient = userRestClient;
        this.rewardTransactionRepository = rewardTransactionRepository;
    }

    @Override
    public Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId,
                                                                     String initiativeId,
                                                                     String fiscalCode,
                                                                     String status,
                                                                     String rewardBatchId,
                                                                     RewardBatchTrxStatus rewardBatchTrxStatus,
                                                                     Pageable pageable) {

        return getMerchantTransactionDTOs2Count(merchantId, initiativeId, fiscalCode, status, rewardBatchId, rewardBatchTrxStatus, pageable)
                .map(tuple -> {
                    Page<MerchantTransactionDTO> page = new PageImpl<>(tuple.getT1(),
                            pageable, tuple.getT2());
                    return new MerchantTransactionsListDTO(tuple.getT1(), page.getNumber(), page.getSize(),
                            (int) page.getTotalElements(), page.getTotalPages());
                });
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs2Count(String merchantId,
                                                                                              String initiativeId,
                                                                                              String fiscalCode,
                                                                                              String status,
                                                                                              String rewardBatchId,
                                                                                              RewardBatchTrxStatus rewardBatchTrxStatus,
                                                                                              Pageable pageable) {
        if (StringUtils.isNotBlank(fiscalCode)){
            return Mono.just(fiscalCode)
                    .flatMap(userRestClient::retrieveFiscalCodeInfo)
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId -> {
                        TrxFiltersDTO filters = new TrxFiltersDTO(merchantId, initiativeId, status, userId, rewardBatchId, rewardBatchTrxStatus);
                        return getMerchantTransactionDTOs(filters, fiscalCode, pageable);
                    });
        } else {
            TrxFiltersDTO filters = new TrxFiltersDTO(merchantId, initiativeId, status, null, rewardBatchId, rewardBatchTrxStatus);
            return getMerchantTransactionDTOs(filters, fiscalCode, pageable);
        }
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs(TrxFiltersDTO filters,
                                                                                        String fiscalCode,
                                                                                        Pageable pageable) {
        return rewardTransactionRepository.findByFilter(filters.getMerchantId(), filters.getInitiativeId(), filters.getUserId(), filters.getStatus(), filters.getRewardBatchId(), filters.getRewardBatchTrxStatus(), pageable)
                .flatMap(t -> createMerchantTransactionDTO(filters.getInitiativeId(), t, fiscalCode))
                .collectSortedList(Comparator.comparing(MerchantTransactionDTO::getElaborationDateTime).reversed())
                .zipWith(rewardTransactionRepository.getCount(filters.getMerchantId(), filters.getInitiativeId(), null, null, filters.getUserId(), filters.getStatus()));
    }

    private Mono<MerchantTransactionDTO> createMerchantTransactionDTO(String initiativeId,
                                                                      RewardTransaction transaction,
                                                                      String fiscalCode) {

        long rewardAmountCents = 0L;
        if (transaction.getRewards() != null
                && transaction.getRewards().get(initiativeId) != null
                && transaction.getRewards().get(initiativeId).getAccruedRewardCents() != null) {

            rewardAmountCents = Math.abs(transaction.getRewards()
                    .get(initiativeId)
                    .getAccruedRewardCents());
        }

        Long effectiveAmountCents = transaction.getEffectiveAmountCents() != null
                ? transaction.getEffectiveAmountCents()
                : transaction.getAmountCents();

        MerchantTransactionDTO out = MerchantTransactionDTO.builder()
                .trxId(transaction.getId())
                .fiscalCode(null)
                .effectiveAmountCents(effectiveAmountCents)
                .rewardAmountCents(rewardAmountCents)
                .rewardBatchId(transaction.getRewardBatchId())
                .rewardBatchTrxStatus(transaction.getRewardBatchTrxStatus())
                .rewardBatchRejectionReason(transaction.getRewardBatchRejectionReason())
                .rewardBatchInclusionDate(transaction.getRewardBatchInclusionDate())
                .franchiseName(transaction.getFranchiseName())
                .pointOfSaleType(transaction.getPointOfSaleType())
                .businessName(transaction.getBusinessName())
                .trxDate(transaction.getTrxDate())
                .elaborationDateTime(transaction.getElaborationDateTime()) // esce come "updateDate"
                .status(transaction.getStatus())
                .channel(transaction.getChannel())
                .build();

        if (StringUtils.isNotBlank(fiscalCode)) {
            out.setFiscalCode(fiscalCode);
            return Mono.just(out);
        } else {
            return userRestClient.retrieveUserInfo(transaction.getUserId())
                    .map(UserInfoPDV::getPii)
                    .doOnNext(out::setFiscalCode)
                    .then(Mono.just(out));
        }
    }

}
