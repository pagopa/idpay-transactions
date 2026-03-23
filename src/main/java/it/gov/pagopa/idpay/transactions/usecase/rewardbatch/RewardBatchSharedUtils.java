package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.nimbusds.jose.util.Pair;
import it.gov.pagopa.common.web.exception.InvalidChecksErrorException;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_CHECKS_ERROR;

@UtilityClass
@Slf4j
public class RewardBatchSharedUtils {

    public static final String OPERATOR_1 = "operator1";
    public static final String OPERATOR_2 = "operator2";
    public static final String OPERATOR_3 = "operator3";
    public static final Set<String> OPERATORS = Set.of(OPERATOR_1, OPERATOR_2, OPERATOR_3);

    public static final String REWARD_BATCHES_PATH_STORAGE_FORMAT = "initiative/%s/merchant/%s/batch/%s/";
    public static final DateTimeFormatter BATCH_MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM", Locale.ITALIAN);

    public static boolean isOperator(String role) {
        return role != null && OPERATORS.contains(role.toLowerCase());
    }

    public static void validChecksError(ChecksErrorDTO dto) {
        if (dto == null) return;

        boolean anyTrue =
                dto.isCfError() ||
                        dto.isProductEligibilityError() ||
                        dto.isDisposalRaeeError() ||
                        dto.isPriceError() ||
                        dto.isBonusError() ||
                        dto.isSellerReferenceError() ||
                        dto.isAccountingDocumentError() ||
                        dto.isGenericError();

        if (!anyTrue) {
            throw new InvalidChecksErrorException(ERROR_MESSAGE_INVALID_CHECKS_ERROR);
        }
    }

    public static ReasonDTO generateReasonDto(TransactionsRequest request) {
        LocalDateTime now = LocalDateTime.now();
        return new ReasonDTO(now, request.getReason());
    }

    public static void checkAndUpdateTrxElaborated(BatchCountersDTO acc, Pair<RewardTransaction, String> trxOld2ActualRewardBatchMonth, RewardTransaction trxOld) {
        if (trxOld.getRewardBatchLastMonthElaborated() != null &&
                (getYearMonth(trxOld.getRewardBatchLastMonthElaborated()).isBefore(getYearMonth(trxOld2ActualRewardBatchMonth.getRight())))) {
            acc.incrementTrxElaborated();
        }
    }

    public static void suspendedTransactionAlreadySuspended(BatchCountersDTO acc, Pair<RewardTransaction, String> trxOld2ActualRewardBatch, RewardTransaction trxOld) {
        if (trxOld.getRewardBatchLastMonthElaborated() != null &&
                (getYearMonth(trxOld.getRewardBatchLastMonthElaborated()).isBefore(getYearMonth(trxOld2ActualRewardBatch.getRight())))) {
            log.info("Handler counters for transaction {} with status SUSPENDED", trxOld.getId());
            acc.incrementTrxElaborated();
        } else {
            log.info("Skipping  handler  for transaction  {}:  status  is already  SUSPENDED", trxOld.getId());
        }
    }

    public static YearMonth getYearMonth(String yearMonthString) {
        return YearMonth.parse(yearMonthString.toLowerCase(), BATCH_MONTH_FORMAT);
    }

    public static String addOneMonth(String yearMonthString) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
        YearMonth yearMonth = YearMonth.parse(yearMonthString, inputFormatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(inputFormatter);
    }

    public static String addOneMonthToItalian(String italianMonthString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM yyyy", Locale.ITALIAN);
        YearMonth yearMonth = YearMonth.parse(italianMonthString, formatter);
        YearMonth nextYearMonth = yearMonth.plusMonths(1);
        return nextYearMonth.format(formatter);
    }

    public static Mono<Void> processBatchesOrchestrator(
            RewardBatchRepository rewardBatchRepository,
            String initiativeId,
            List<String> rewardBatchIds,
            it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus statusIfEmpty,
            BiFunction<String, String, Mono<?>> businessLogic) {

        Flux<String> idsFlux;

        if (rewardBatchIds != null && !rewardBatchIds.isEmpty()) {
            idsFlux = Flux.fromIterable(rewardBatchIds);
        } else {
            idsFlux = rewardBatchRepository.findRewardBatchByStatus(statusIfEmpty)
                    .collectList()
                    .flatMapMany(batchList -> {
                        if (batchList.isEmpty()) {
                            log.warn("No batches found with status {} to process.", statusIfEmpty);
                            return Flux.empty();
                        }
                        log.info("Found {} batches with status {} to process.", batchList.size(), statusIfEmpty);
                        return Flux.fromIterable(batchList);
                    })
                    .map(RewardBatch::getId);
        }

        return idsFlux
                .doOnNext(id -> log.info("Processing batch {}", id))
                .concatMap(id -> businessLogic.apply(id, initiativeId)
                        .onErrorResume(error -> {
                            log.error("Failed to process batch {}: {}", id, error.getMessage(), error);
                            return Mono.empty();
                        })
                )
                .then();
    }
}

