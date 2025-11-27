package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.LocalDateTime;
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
                                                                     OrganizationRole organizationRole,
                                                                     String initiativeId,
                                                                     String fiscalCode,
                                                                     String status,
                                                                     String rewardBatchId,
                                                                     String rewardBatchTrxStatus,
                                                                     String pointOfSaleId,
                                                                     Pageable pageable) {

        RewardBatchTrxStatus parsedRewardBatchTrxStatus = parseRewardBatchTrxStatus(rewardBatchTrxStatus);

        RewardBatchTrxStatus effectiveRewardBatchTrxStatus =
                normalizeStatusForRole(parsedRewardBatchTrxStatus, organizationRole);

        TrxFiltersDTO filters = new TrxFiltersDTO(merchantId, initiativeId, fiscalCode, status, rewardBatchId,
                effectiveRewardBatchTrxStatus, pointOfSaleId);

        return getMerchantTransactionDTOs2Count(filters, organizationRole, pageable)
                .map(tuple -> {
                    Page<MerchantTransactionDTO> page = new PageImpl<>(tuple.getT1(),
                            pageable, tuple.getT2());
                    return new MerchantTransactionsListDTO(tuple.getT1(), page.getNumber(), page.getSize(),
                            (int) page.getTotalElements(), page.getTotalPages());
                });
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs2Count(TrxFiltersDTO filters,
                                                                                              OrganizationRole organizationRole,
                                                                                              Pageable pageable) {
        if (StringUtils.isNotBlank(filters.getFiscalCode())){
            return Mono.just(filters.getFiscalCode())
                    .flatMap(userRestClient::retrieveFiscalCodeInfo)
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId -> getMerchantTransactionDTOs(filters, userId, organizationRole, pageable)
                    );
        } else {
            return getMerchantTransactionDTOs(filters, null, organizationRole, pageable);
        }
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs(
            TrxFiltersDTO filters,
            String userId,
            OrganizationRole organizationRole,
            Pageable pageable) {

        return rewardTransactionRepository
                .findByFilter(filters, userId, organizationRole, pageable)
                .flatMap(t -> createMerchantTransactionDTO(filters.getInitiativeId(), t, filters.getFiscalCode()))
                .collectSortedList(Comparator.comparing(MerchantTransactionDTO::getElaborationDateTime).reversed())
                .zipWith(
                        rewardTransactionRepository.getCount(
                                filters,
                                null,
                                null,
                                userId,
                                organizationRole
                        )
                );
    }


    private Mono<MerchantTransactionDTO> createMerchantTransactionDTO(String initiativeId, RewardTransaction transaction, String fiscalCode) {
        MerchantTransactionDTO out = MerchantTransactionDTO.builder()
                .trxId(transaction.getId())
                .effectiveAmountCents(transaction.getAmountCents())
                .rewardAmountCents(transaction.getRewards().get(initiativeId).getAccruedRewardCents())
                .trxDate(transaction.getTrxDate() == null ? LocalDateTime.MIN : transaction.getTrxDate())
                .elaborationDateTime(transaction.getElaborationDateTime())
                .status(transaction.getStatus())
                .channel(transaction.getChannel())
                .trxChargeDate(transaction.getTrxChargeDate())
                .additionalProperties(transaction.getAdditionalProperties())
                .trxCode(transaction.getTrxCode())
                .authorizedAmountCents(transaction.getAmountCents() - transaction.getRewards().get(initiativeId).getAccruedRewardCents())
                .docNumber(transaction.getInvoiceData() != null ? transaction.getInvoiceData().getDocNumber() : null)
                .fileName(transaction.getInvoiceData() != null ? transaction.getInvoiceData().getFilename() : null)
                .rewardBatchTrxStatus(transaction.getRewardBatchTrxStatus())
                .pointOfSaleId(transaction.getPointOfSaleId() == null ? "-" : transaction.getPointOfSaleId())
                .rewardBatchRejectionReason(transaction.getRewardBatchRejectionReason() == null ? "-" : transaction.getRewardBatchRejectionReason())
                .franchiseName(transaction.getFranchiseName() == null ? "-" : transaction.getFranchiseName())

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

    private RewardBatchTrxStatus normalizeStatusForRole(RewardBatchTrxStatus status,
                                                        OrganizationRole organizationRole) {
        if (organizationRole == OrganizationRole.MERCHANT && status == RewardBatchTrxStatus.TO_CHECK) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Status TO_CHECK not allowed for merchants");
        }
        return status;
    }

    private RewardBatchTrxStatus parseRewardBatchTrxStatus(String rewardBatchTrxStatus) {
        if (StringUtils.isBlank(rewardBatchTrxStatus)) {
            return null;
        }
        try {
            return RewardBatchTrxStatus.valueOf(rewardBatchTrxStatus);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid rewardBatchTrxStatus value: " + rewardBatchTrxStatus);
        }
    }

}
