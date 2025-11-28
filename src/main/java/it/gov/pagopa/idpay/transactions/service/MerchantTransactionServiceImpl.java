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
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.LocalDateTime;
import java.util.*;
import java.util.Comparator;

@Service
public class MerchantTransactionServiceImpl implements MerchantTransactionService {
    private final UserRestClient userRestClient;
    private final RewardTransactionRepository rewardTransactionRepository;

    private static final Set<String> OPERATORS =
            Set.of("operator1", "operator2", "operator3");

    protected MerchantTransactionServiceImpl(
            UserRestClient userRestClient, RewardTransactionRepository rewardTransactionRepository) {
        this.userRestClient = userRestClient;
        this.rewardTransactionRepository = rewardTransactionRepository;
    }

    @Override
    public Mono<MerchantTransactionsListDTO> getMerchantTransactions(String merchantId,
                                                                     String organizationRole,
                                                                     String initiativeId,
                                                                     String fiscalCode,
                                                                     String status,
                                                                     String rewardBatchId,
                                                                     String rewardBatchTrxStatus,
                                                                     String pointOfSaleId,
                                                                     Pageable pageable) {

        RewardBatchTrxStatus parsedRewardBatchTrxStatus = parseRewardBatchTrxStatus(rewardBatchTrxStatus);

        TrxFiltersDTO filters = new TrxFiltersDTO(
                merchantId,
                initiativeId,
                fiscalCode,
                status,
                rewardBatchId,
                parsedRewardBatchTrxStatus,
                pointOfSaleId
        );

        return getMerchantTransactionDTOs2Count(filters, organizationRole, pageable)
                .map(tuple -> {
                    Page<MerchantTransactionDTO> page = new PageImpl<>(tuple.getT1(),
                            pageable, tuple.getT2());
                    return new MerchantTransactionsListDTO(tuple.getT1(), page.getNumber(), page.getSize(),
                            (int) page.getTotalElements(), page.getTotalPages());
                });
    }

    @Override
    public Mono<List<String>> getProcessedTransactionStatuses(
            String organizationRole) {

        List<String> allStatuses = Arrays.stream(RewardBatchTrxStatus.values())
                .map(Enum::name)
                .toList();

        if (isOperator(organizationRole)) {
            return Mono.just(allStatuses);
        } else {
            return Mono.just(
                    allStatuses.stream()
                            .filter(s -> !"TO_CHECK".equalsIgnoreCase(s))
                            .toList()
            );
        }
    }

    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs2Count(
            TrxFiltersDTO filters,
            String organizationRole,
            Pageable pageable) {

        TrxFiltersDTO effectiveFilters;
        if (!isOperator(organizationRole)
                && filters.getRewardBatchTrxStatus() == RewardBatchTrxStatus.CONSULTABLE) {
            effectiveFilters = new TrxFiltersDTO(
                    filters.getMerchantId(),
                    filters.getInitiativeId(),
                    filters.getFiscalCode(),
                    filters.getStatus(),
                    filters.getRewardBatchId(),
                    null,
                    filters.getPointOfSaleId()
            );
        } else {
            effectiveFilters = filters;
        }

        if (StringUtils.isNotBlank(effectiveFilters.getFiscalCode())) {
            return userRestClient.retrieveFiscalCodeInfo(effectiveFilters.getFiscalCode())
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId -> getMerchantTransactionDTOs(effectiveFilters, userId, organizationRole, pageable));
        } else {
            return getMerchantTransactionDTOs(effectiveFilters, null, organizationRole, pageable);
        }
    }


    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs(
            TrxFiltersDTO filters,
            String userId,
            String organizationRole,
            Pageable pageable) {

        return rewardTransactionRepository
                .findByFilter(filters, userId, pageable)
                .flatMap(t -> createMerchantTransactionDTO(
                        filters.getInitiativeId(),
                        t,
                        filters.getFiscalCode(),
                        organizationRole
                ))
                .collectSortedList(Comparator.comparing(MerchantTransactionDTO::getElaborationDateTime).reversed())
                .zipWith(
                        rewardTransactionRepository.getCount(
                                filters,
                                null,
                                null,
                                userId
                        )
                );
    }

    private Mono<MerchantTransactionDTO> createMerchantTransactionDTO(
            String initiativeId,
            RewardTransaction transaction,
            String fiscalCode,
            String organizationRole) {

        RewardBatchTrxStatus original = transaction.getRewardBatchTrxStatus();
        RewardBatchTrxStatus exposed = original;

        if (!isOperator(organizationRole) && original == RewardBatchTrxStatus.TO_CHECK) {
            exposed = RewardBatchTrxStatus.CONSULTABLE;
        }

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
                .authorizedAmountCents(transaction.getAmountCents()
                            - transaction.getRewards().get(initiativeId).getAccruedRewardCents())
                .docNumber(transaction.getInvoiceData() != null ? transaction.getInvoiceData().getDocNumber() : null)
                .fileName(transaction.getInvoiceData() != null ? transaction.getInvoiceData().getFilename() : null)
                .rewardBatchTrxStatus(exposed)
                .pointOfSaleId(transaction.getPointOfSaleId() == null ? "-" : transaction.getPointOfSaleId())
                .rewardBatchRejectionReason(transaction.getRewardBatchRejectionReason() == null ? "-" : transaction.getRewardBatchRejectionReason())
                .franchiseName(transaction.getFranchiseName() == null ? "-" : transaction.getFranchiseName())
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

    private boolean isOperator(String role) {
        return role != null && OPERATORS.contains(role.toLowerCase());
    }
}
