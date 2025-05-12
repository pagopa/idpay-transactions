package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
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
    public Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId, String initiativeId, String fiscalCode, String status, Pageable pageable) {

        return getMerchantTransactionDTOs2Count(merchantId, initiativeId, fiscalCode, status, pageable)
                .map(tuple -> {
                    Page<MerchantTransactionDTO> page = new PageImpl<>(tuple.getT1(),
                            pageable, tuple.getT2());
                    return new MerchantTransactionsListDTO(tuple.getT1(), page.getNumber(), page.getSize(),
                            (int) page.getTotalElements(), page.getTotalPages());
                });
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs2Count(String merchantId, String initiativeId, String fiscalCode, String status, Pageable pageable) {
        if (StringUtils.isNotBlank(fiscalCode)){
            return Mono.just(fiscalCode)
                    .flatMap(userRestClient::retrieveFiscalCodeInfo)
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId -> getMerchantTransactionDTOs(merchantId, initiativeId, status, pageable, userId, fiscalCode));
        } else {
            return getMerchantTransactionDTOs(merchantId, initiativeId, status, pageable, null, fiscalCode);
        }
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs(String merchantId, String initiativeId, String status, Pageable pageable, String userId, String fiscalCode) {
        return rewardTransactionRepository.findByFilter(merchantId, initiativeId, userId, status, pageable)
                .flatMap(t -> createMerchantTransactionDTO(initiativeId, t, fiscalCode))
                .collectSortedList(Comparator.comparing(MerchantTransactionDTO::getElaborationDateTime).reversed())
                .zipWith(rewardTransactionRepository.getCount(merchantId, initiativeId, userId, status));
    }

    private Mono<MerchantTransactionDTO> createMerchantTransactionDTO(String initiativeId, RewardTransaction transaction, String fiscalCode) {
        MerchantTransactionDTO out = MerchantTransactionDTO.builder()
                .trxId(transaction.getId())
                .effectiveAmountCents(transaction.getAmountCents())
                .rewardAmountCents(transaction.getRewards().get(initiativeId).getAccruedRewardCents())
                .trxDate(transaction.getTrxDate())
                .elaborationDateTime(transaction.getElaborationDateTime())
                .status(transaction.getStatus())
                .channel(transaction.getChannel())
                .build();

        if (StringUtils.isNotBlank(fiscalCode)){
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
