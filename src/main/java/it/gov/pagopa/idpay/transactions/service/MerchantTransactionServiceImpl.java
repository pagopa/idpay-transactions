package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.OffsetDateTime;
import java.util.*;

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

        if (pageable.getSort().isUnsorted()) {
            pageable = PageRequest.of(
                    pageable.getPageNumber(),
                    pageable.getPageSize(),
                    Sort.by(Sort.Direction.DESC, "rewardBatchTrxStatus")
            );
        }


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

        Pageable finalPageable = pageable;
        return getMerchantTransactionDTOs2Count(filters, organizationRole, pageable)
                .map(tuple -> {
                    Page<MerchantTransactionDTO> page = new PageImpl<>(tuple.getT1(),
                            finalPageable, tuple.getT2());
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

        boolean includeToCheckWithConsultable =
                !isOperator(organizationRole)
                        && filters.getRewardBatchTrxStatus() == RewardBatchTrxStatus.CONSULTABLE;

        if (StringUtils.isNotBlank(filters.getFiscalCode())) {
            return userRestClient.retrieveFiscalCodeInfo(filters.getFiscalCode())
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId ->
                            getMerchantTransactionDTOs(
                                    filters,
                                    userId,
                                    organizationRole,
                                    pageable,
                                    includeToCheckWithConsultable
                            )
                    );
        } else {
            return getMerchantTransactionDTOs(
                    filters,
                    null,
                    organizationRole,
                    pageable,
                    includeToCheckWithConsultable
            );
        }
    }



    private Mono<Tuple2<List<MerchantTransactionDTO>, Long>> getMerchantTransactionDTOs(
            TrxFiltersDTO filters,
            String userId,
            String organizationRole,
            Pageable pageable,
            boolean includeToCheckWithConsultable) {

        return rewardTransactionRepository
                .findByFilter(filters, userId, includeToCheckWithConsultable, pageable)
                .concatMap(t -> createMerchantTransactionDTO(
                        filters.getInitiativeId(),
                        t,
                        filters.getFiscalCode(),
                        organizationRole
                ))
                .collectList()
                .zipWith(
                        rewardTransactionRepository.getCount(
                                filters,
                                filters.getPointOfSaleId(),
                                null,
                                userId,
                                includeToCheckWithConsultable
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
                .fiscalCode(transaction.getFiscalCode() != null ? transaction.getFiscalCode() : "-")
                .effectiveAmountCents(transaction.getAmountCents())
                .rewardAmountCents(transaction.getRewards().get(initiativeId).getAccruedRewardCents())
                .trxDate(transaction.getTrxDate() == null ? OffsetDateTime.MIN : transaction.getTrxDate())
                .elaborationDateTime(transaction.getElaborationDateTime())
                .status(transaction.getStatus())
                .channel(transaction.getChannel())
                .trxChargeDate(transaction.getTrxChargeDate())
                .additionalProperties(transaction.getAdditionalProperties())
                .trxCode(transaction.getTrxCode())
                .authorizedAmountCents(transaction.getAmountCents()
                            - transaction.getRewards().get(initiativeId).getAccruedRewardCents())
                .invoiceData(transaction.getInvoiceData() != null ? transaction.getInvoiceData() : new InvoiceData())
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
