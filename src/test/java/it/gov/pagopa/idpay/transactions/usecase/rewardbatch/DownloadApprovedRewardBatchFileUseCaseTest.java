package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.storage.ApprovedRewardBatchBlobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.MERCHANT_OR_OPERATOR_HEADER_MANDATORY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DownloadApprovedRewardBatchFileUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private ApprovedRewardBatchBlobService approvedRewardBatchBlobService;
    private DownloadApprovedRewardBatchFileUseCase useCase;
    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String BATCH_ID = "BATCH_ID";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String OP1 = "operator1";
    private static final String OP2 = "operator2";

    @BeforeEach
    void setup() { useCase = new DownloadApprovedRewardBatchFileUseCase(rewardBatchRepository, approvedRewardBatchBlobService); }

    @Test
    void execute_invalidRequest_missingHeaders() {
        StepVerifier.create(useCase.execute(null, null, INITIATIVE_ID, BATCH_ID))
                .expectErrorSatisfies(ex -> { assertInstanceOf(RewardBatchInvalidRequestException.class, ex); assertEquals(MERCHANT_OR_OPERATOR_HEADER_MANDATORY, ex.getMessage()); }).verify();
    }

    @Test
    void execute_notFound_merchantPath() {
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID)).expectError(RewardBatchNotFound.class).verify();
    }

    @Test
    void execute_roleNotAllowed_whenMerchantNull() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        StepVerifier.create(useCase.execute(null, "guest", INITIATIVE_ID, BATCH_ID)).expectError(RoleNotAllowedException.class).verify();
    }

    @Test
    void execute_notApproved() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));
        StepVerifier.create(useCase.execute(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID)).expectError(RewardBatchNotApprovedException.class).verify();
    }

    @Test
    void execute_allowed_whenRefundState() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));
        when(approvedRewardBatchBlobService.getFileSignedUrl(anyString())).thenReturn("signed-refund");
        StepVerifier.create(useCase.execute(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .assertNext(r -> assertEquals("signed-refund", r.getApprovedBatchUrl())).verifyComplete();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "   "})
    void execute_missingFilename(String invalid) {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename(invalid).merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));
        StepVerifier.create(useCase.execute(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID)).expectError(RewardBatchMissingFilenameException.class).verify();
    }

    @Test
    void execute_success_merchant() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findByMerchantIdAndId(MERCHANT_ID, BATCH_ID)).thenReturn(Mono.just(batch));
        when(approvedRewardBatchBlobService.getFileSignedUrl(anyString())).thenReturn("signed");
        StepVerifier.create(useCase.execute(MERCHANT_ID, OP1, INITIATIVE_ID, BATCH_ID))
                .assertNext(r -> assertEquals("signed", r.getApprovedBatchUrl())).verifyComplete();
    }

    @Test
    void execute_success_operatorOnlyMerchantNull() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVED).filename("file.csv").merchantId(MERCHANT_ID).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        when(approvedRewardBatchBlobService.getFileSignedUrl(anyString())).thenReturn("signed2");
        StepVerifier.create(useCase.execute(null, OP2, INITIATIVE_ID, BATCH_ID))
                .assertNext(r -> assertEquals("signed2", r.getApprovedBatchUrl())).verifyComplete();
    }
}

