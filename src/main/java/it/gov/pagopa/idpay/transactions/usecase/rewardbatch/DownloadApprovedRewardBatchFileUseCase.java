package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.dto.DownloadRewardBatchResponseDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Set;

import static it.gov.pagopa.idpay.transactions.usecase.rewardbatch.RewardBatchSharedUtils.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;

@Service
@Slf4j
@AllArgsConstructor
public class DownloadApprovedRewardBatchFileUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final ApprovedRewardBatchBlobService approvedRewardBatchBlobService;

    public Mono<DownloadRewardBatchResponseDTO> execute(String merchantId, String organizationRole, String initiativeId, String rewardBatchId) {

        if ((merchantId == null || merchantId.isBlank()) &&
                (organizationRole == null || organizationRole.isBlank())) {
            return Mono.error(new RewardBatchInvalidRequestException(MERCHANT_OR_OPERATOR_HEADER_MANDATORY));
        }

        Mono<RewardBatch> query =
                merchantId == null
                        ? rewardBatchRepository.findById(rewardBatchId)
                        : rewardBatchRepository.findByMerchantIdAndId(merchantId, rewardBatchId);

        return query
                .switchIfEmpty(Mono.error(new RewardBatchNotFound(
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )))
                .map(batch -> {

                    if (merchantId == null && !isValidInvitaliaOperator(organizationRole)) {
                        throw new RoleNotAllowedException(
                                ROLE_NOT_ALLOWED,
                                ERROR_MESSAGE_ROLE_NOT_ALLOWED
                        );
                    }

                    if (!RewardBatchStatus.APPROVED.equals(batch.getStatus()) && !isRefundState(batch.getStatus())) {
                        throw new RewardBatchNotApprovedException(
                                REWARD_BATCH_NOT_APPROVED_OR_REFUNDABLE,
                                ERROR_MESSAGE_REWARD_BATCH_NOT_APPROVED_OR_REFUNDABLE.formatted(rewardBatchId)
                        );
                    }

                    String filename = batch.getFilename();
                    if (filename == null || filename.isBlank()) {
                        throw new RewardBatchMissingFilenameException(
                                REWARD_BATCH_MISSING_FILENAME,
                                ERROR_MESSAGE_REWARD_BATCH_MISSING_FILENAME.formatted(rewardBatchId)
                        );
                    }

                    String blobPath = String.format(
                            REWARD_BATCHES_PATH_STORAGE_FORMAT + "%s",
                            initiativeId,
                            batch.getMerchantId(),
                            rewardBatchId,
                            filename
                    );

                    return DownloadRewardBatchResponseDTO.builder()
                            .approvedBatchUrl(
                                    approvedRewardBatchBlobService.getFileSignedUrl(blobPath)
                            )
                            .build();
                });
    }

    private boolean isValidInvitaliaOperator(String organizationRole) {
        return OPERATOR_1.equals(organizationRole)
                || OPERATOR_2.equals(organizationRole)
                || OPERATOR_3.equals(organizationRole);
    }

    private boolean isRefundState(RewardBatchStatus status) {
        return Set.of(
                RewardBatchStatus.PENDING_REFUND,
                RewardBatchStatus.NOT_REFUNDED,
                RewardBatchStatus.REFUNDED
        ).contains(status);
    }
}

