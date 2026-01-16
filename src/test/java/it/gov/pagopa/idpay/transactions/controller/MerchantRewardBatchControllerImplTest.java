package it.gov.pagopa.idpay.transactions.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
import it.gov.pagopa.idpay.transactions.config.ServiceExceptionConfig;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import java.time.LocalDate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@WebFluxTest(controllers = MerchantRewardBatchControllerImpl.class)
@Import({ServiceExceptionConfig.class})
class MerchantRewardBatchControllerImplTest {

  @Autowired
  protected WebTestClient webClient;

  @MockitoBean
  RewardBatchService rewardBatchService;

  @MockitoBean
  RewardBatchMapper rewardBatchMapper;


  private static final String MERCHANT_ID = "MERCHANT_ID";
  private static final String INITIATIVE_ID = "INIT1";
    private static final String REWARD_BATCH_ID_1 = "REWARD_BATCH_ID_1";
    private static final String REWARD_BATCH_ID_2 = "REWARD_BATCH_ID_2";

    private static final List<String> BATCH_IDS = Arrays.asList(REWARD_BATCH_ID_1, REWARD_BATCH_ID_2);

    private static final String FAKE_FILENAME = "test/path/report.csv";
    @Test
    void generateAndSaveCsv_Success_Accepted() {

        when(rewardBatchService.generateAndSaveCsv(REWARD_BATCH_ID_1, INITIATIVE_ID, MERCHANT_ID))
                .thenReturn(Mono.just(FAKE_FILENAME));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/generateAndSaveCsv?merchantId={merchantId}",
                        INITIATIVE_ID, REWARD_BATCH_ID_1, MERCHANT_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo(FAKE_FILENAME);

        verify(rewardBatchService).generateAndSaveCsv(REWARD_BATCH_ID_1, INITIATIVE_ID, MERCHANT_ID);
    }

    @Test
    void generateAndSaveCsv_ServiceFails_InternalServerError() {

        RuntimeException serviceException = new RuntimeException();
        when(rewardBatchService.generateAndSaveCsv(REWARD_BATCH_ID_1, INITIATIVE_ID, MERCHANT_ID))
                .thenReturn(Mono.error(serviceException));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/generateAndSaveCsv?merchantId={merchantId}",
                        INITIATIVE_ID, REWARD_BATCH_ID_1, MERCHANT_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody();

        verify(rewardBatchService).generateAndSaveCsv(REWARD_BATCH_ID_1, INITIATIVE_ID, MERCHANT_ID);
    }

    @Test
    void rewardBatchConfirmationBatch_WithValidList() {
        RewardBatchesRequest request = new RewardBatchesRequest(BATCH_IDS);
        when(rewardBatchService.rewardBatchConfirmationBatch(INITIATIVE_ID, BATCH_IDS))
                .thenReturn(Mono.empty());
        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/approved", INITIATIVE_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody().isEmpty();
        verify(rewardBatchService, times(1))
                .rewardBatchConfirmationBatch(INITIATIVE_ID, BATCH_IDS);
    }

        @Test
        void rewardBatchConfirmationBatch_WhenRequestListIsNull() {
            RewardBatchesRequest request = new RewardBatchesRequest(null);
            when(rewardBatchService.rewardBatchConfirmationBatch(INITIATIVE_ID, BATCH_IDS))
                    .thenReturn(Mono.empty());
            webClient.post()
                    .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/approved", INITIATIVE_ID)
                    .bodyValue(request)
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody().isEmpty();
            verify(rewardBatchService, times(1))
                    .rewardBatchConfirmationBatch(
                            INITIATIVE_ID,
                            List.of()
                    );

        }

        @Test
        void rewardBatchConfirmationBatch_WhenRequestListIsEmpty() {
            RewardBatchesRequest request = new RewardBatchesRequest(Collections.emptyList());

            when(rewardBatchService.rewardBatchConfirmationBatch(INITIATIVE_ID, BATCH_IDS))
                    .thenReturn(Mono.empty());

            webClient.post()
                    .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/approved", INITIATIVE_ID)
                    .bodyValue(request)
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody().isEmpty();

            verify(rewardBatchService, times(1))
                    .rewardBatchConfirmationBatch(
                            INITIATIVE_ID,
                            Collections.emptyList()
                    );

    }

  @Test
  void getRewardBatchesForMerchantOk() {
    RewardBatch batch = RewardBatch.builder()
        .id("BATCH1")
        .name("Reward Batch 1")
        .build();

    Page<RewardBatch> page = new PageImpl<>(
        List.of(batch),
        PageRequest.of(0, 10),
        1
    );

    RewardBatchDTO dto = RewardBatchDTO.builder()
        .id(batch.getId())
        .name(batch.getName())
        .build();

    when(rewardBatchService.getRewardBatches(
        eq(MERCHANT_ID),
        isNull(),
        isNull(),
        isNull(),
        isNull(),
        any(Pageable.class)))
        .thenReturn(Mono.just(page));

    when(rewardBatchMapper.toDTO(batch))
        .thenReturn(Mono.just(dto));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches")
            .queryParam("page", 0)
            .queryParam("size", 10)
            .build(INITIATIVE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(RewardBatchListDTO.class)
        .value(response -> {
          assertNotNull(response);
          assertEquals(1, response.getContent().size());
          assertEquals("BATCH1", response.getContent().getFirst().getId());
          assertEquals("Reward Batch 1", response.getContent().getFirst().getName());
          assertEquals(1, response.getTotalElements());
          assertEquals(1, response.getTotalPages());
          assertEquals(10, response.getPageSize());
        });

    verify(rewardBatchService, times(1))
        .getRewardBatches(eq(MERCHANT_ID), isNull(), isNull(), isNull(), isNull(), any(Pageable.class));
    verify(rewardBatchMapper, times(1)).toDTO(batch);
  }

  @Test
  void getRewardBatchesForOperatorOk() {
    RewardBatch batch = RewardBatch.builder()
        .id("BATCH1")
        .name("Reward Batch 1")
        .build();

    Page<RewardBatch> page = new PageImpl<>(
        List.of(batch),
        PageRequest.of(0, 10),
        1
    );

    RewardBatchDTO dto = RewardBatchDTO.builder()
        .id(batch.getId())
        .name(batch.getName())
        .build();

    String organizationRole = "OPERATOR";

    when(rewardBatchService.getRewardBatches(
        isNull(),
        eq(organizationRole),
        isNull(),
        isNull(),
        isNull(),
        any(Pageable.class)))
        .thenReturn(Mono.just(page));

    when(rewardBatchMapper.toDTO(batch))
        .thenReturn(Mono.just(dto));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches")
            .queryParam("page", 0)
            .queryParam("size", 10)
            .build(INITIATIVE_ID))
        .header("x-organization-role", organizationRole)
        .exchange()
        .expectStatus().isOk()
        .expectBody(RewardBatchListDTO.class)
        .value(response -> {
          assertNotNull(response);
          assertEquals(1, response.getContent().size());
          assertEquals("BATCH1", response.getContent().getFirst().getId());
          assertEquals("Reward Batch 1", response.getContent().getFirst().getName());
          assertEquals(1, response.getTotalElements());
          assertEquals(1, response.getTotalPages());
          assertEquals(10, response.getPageSize());
        });

    verify(rewardBatchService, times(1))
        .getRewardBatches(isNull(), eq(organizationRole), isNull(), isNull(), isNull(), any(Pageable.class));
    verify(rewardBatchMapper, times(1)).toDTO(batch);
  }

  @Test
  void sendRewardBatchesOk() {

    String batchId = "BATCH1";

    when(rewardBatchService.sendRewardBatch(MERCHANT_ID, batchId))
        .thenReturn(Mono.empty());

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{batchId}/send")
            .build(INITIATIVE_ID, batchId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isNoContent();

    verify(rewardBatchService, times(1))
        .sendRewardBatch(MERCHANT_ID, batchId);
  }

  @Test
  void getRewardBatches_shouldThrowBadRequest_whenNoMerchantAndNoRole() {
    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches")
            .queryParam("page", 0)
            .queryParam("size", 10)
            .build(INITIATIVE_ID))
        .exchange()
        .expectStatus().isBadRequest()
        .expectBody()
        .consumeWith(response -> {
            assertNotNull(response.getResponseBody());
            String body = new String(response.getResponseBody());
            assertTrue(body.contains(ExceptionMessage.MISSING_TRANSACTIONS_FILTERS));
        });
  }

    @Test
    void suspendTransactionsOk() {
        String rewardBatchId = "BATCH1";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1", "trx2"));
        request.setReason("Test reason");

        RewardBatch batch = RewardBatch.builder()
                .id(rewardBatchId)
                .status(RewardBatchStatus.CREATED)
                .build();

        RewardBatchDTO dto = RewardBatchDTO.builder()
                .id(rewardBatchId)
                .build();

        when(rewardBatchService.suspendTransactions(rewardBatchId, INITIATIVE_ID, request))
                .thenReturn(Mono.just(batch));
        when(rewardBatchMapper.toDTO(batch)).thenReturn(Mono.just(dto));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/suspended",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(RewardBatchDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertEquals(rewardBatchId, res.getId());
                });

        verify(rewardBatchService, times(1))
                .suspendTransactions(rewardBatchId, INITIATIVE_ID, request);
        verify(rewardBatchMapper, times(1)).toDTO(batch);
    }


    @Test
    void suspendTransactionsBatchApproved_shouldReturnError() {
        String rewardBatchId = "BATCH2";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1"));
        request.setReason("Test reason");

        when(rewardBatchService.suspendTransactions(rewardBatchId, INITIATIVE_ID, request))
                .thenReturn(Mono.error(new IllegalStateException("Cannot suspend transactions on an APPROVED batch")));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/suspended",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody();

        verify(rewardBatchService, times(1))
                .suspendTransactions(rewardBatchId, INITIATIVE_ID, request);
        verifyNoInteractions(rewardBatchMapper);
    }
    @Test
    void rewardBatchConfirmation_Success() {
        String rewardBatchId = "BATCH1";
        RewardBatch batch = RewardBatch.builder()
                .id(rewardBatchId)
                .name("Reward Batch 1")
                .build();

        when(rewardBatchService.rewardBatchConfirmation(INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.just(batch));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchid}/approved", INITIATIVE_ID, rewardBatchId)
                .exchange()
                .expectStatus().isOk() // 200 OK
                .expectBody(RewardBatch.class)
                .isEqualTo(batch);

        verify(rewardBatchService, times(1)).rewardBatchConfirmation(INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void rewardBatchConfirmation_BatchNotFound() {
        String rewardBatchId = "BATCH2";

        when(rewardBatchService.rewardBatchConfirmation(INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND)));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchid}/approved", INITIATIVE_ID, rewardBatchId)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message").isEqualTo("REWARD_BATCH_NOT_FOUND");

        verify(rewardBatchService, times(1)).rewardBatchConfirmation(INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void rewardBatchConfirmation_BatchAlreadyApproved() {
        String rewardBatchId = "BATCH3";

        when(rewardBatchService.rewardBatchConfirmation(INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_ALREADY_APPROVED)));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchid}/approved", INITIATIVE_ID, rewardBatchId)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("REWARD_BATCH_ALREADY_APPROVED");

        verify(rewardBatchService, times(1)).rewardBatchConfirmation(INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void approvedTransactionsOk() {
        String rewardBatchId = "BATCH";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1", "trx2"));

        RewardBatch batch = RewardBatch.builder()
                .id(rewardBatchId)
                .status(RewardBatchStatus.APPROVED)
                .build();

        RewardBatchDTO dto = RewardBatchDTO.builder()
                .id(rewardBatchId)
                .build();

        when(rewardBatchService.approvedTransactions(rewardBatchId, request, INITIATIVE_ID))
                .thenReturn(Mono.just(batch));
        when(rewardBatchMapper.toDTO(batch)).thenReturn(Mono.just(dto));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/approved",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(RewardBatchDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertEquals(rewardBatchId, res.getId());
                });

        verify(rewardBatchService, times(1))
                .approvedTransactions(any(), any(), any());
        verify(rewardBatchMapper, times(1)).toDTO(batch);
    }

    @Test
    void rejectTransactionsOk() {
        String rewardBatchId = "BATCH";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1", "trx2"));
        request.setReason("reason");

        RewardBatch batch = RewardBatch.builder()
                .id(rewardBatchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        RewardBatchDTO dto = RewardBatchDTO.builder()
                .id(rewardBatchId)
                .build();

        when(rewardBatchService.rejectTransactions(rewardBatchId, INITIATIVE_ID, request))
                .thenReturn(Mono.just(batch));
        when(rewardBatchMapper.toDTO(batch)).thenReturn(Mono.just(dto));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/rejected",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-merchant-id", MERCHANT_ID)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(RewardBatchDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertEquals(rewardBatchId, res.getId());
                });

        verify(rewardBatchService, times(1))
                .rejectTransactions(any(), any(), any());
        verify(rewardBatchMapper, times(1)).toDTO(batch);
    }

    @Test
    void evaluatingRewardBatches() {
        RewardBatchesRequest batchRequest = RewardBatchesRequest.builder().rewardBatchIds(List.of("BATCH_ID")).build();

        when(rewardBatchService.evaluatingRewardBatches(List.of("BATCH_ID"))).thenReturn(Mono.just(1L));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/evaluate",
                        INITIATIVE_ID)
                .bodyValue(batchRequest)
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void evaluatingRewardBatches_notFound() {
        RewardBatchesRequest batchRequest =
                RewardBatchesRequest.builder()
                        .rewardBatchIds(List.of("BATCH_ID"))
                        .build();

        when(rewardBatchService.evaluatingRewardBatches(List.of("BATCH_ID")))
                .thenReturn(Mono.error(
                        new RewardBatchNotFound("DUMMY_EXCEPTION", "MESSAGE_DUMMY"))
                );

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/evaluate",
                        INITIATIVE_ID)
                .bodyValue(batchRequest)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody(ErrorDTO.class)
                .value(errorDto -> {
                    Assertions.assertEquals("DUMMY_EXCEPTION", errorDto.getCode());
                    Assertions.assertEquals("MESSAGE_DUMMY", errorDto.getMessage());
                });
    }

    @Test
    void validateRewardBatch_L1ToL2_Success() {
        String rewardBatchId = "BATCH1";

        when(rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.empty());

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/validated",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-organization-role", "operator1")
                .exchange()
                .expectStatus().isOk();

        verify(rewardBatchService, times(1)).validateRewardBatch("operator1", INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void validateRewardBatch_L1ToL2_RoleNotAllowed() {
        String rewardBatchId = "BATCH2";

        when(rewardBatchService.validateRewardBatch("wrongRole", INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.error(new RewardBatchException(HttpStatus.FORBIDDEN,
                        ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED_FOR_L1_PROMOTION)));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/validated",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-organization-role", "wrongRole")
                .exchange()
                .expectStatus().isForbidden()
                .expectBody()
                .jsonPath("$.message").isEqualTo(ExceptionConstants.ExceptionCode.ROLE_NOT_ALLOWED_FOR_L1_PROMOTION);

        verify(rewardBatchService, times(1)).validateRewardBatch("wrongRole", INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void validateRewardBatch_L1ToL2_LessThan15Percent() {
        String rewardBatchId = "BATCH3";

        when(rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                        ExceptionConstants.ExceptionCode.BATCH_NOT_ELABORATED_15_PERCENT)));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/validated",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-organization-role", "operator1")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo(ExceptionConstants.ExceptionCode.BATCH_NOT_ELABORATED_15_PERCENT);

        verify(rewardBatchService, times(1)).validateRewardBatch("operator1", INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void validateRewardBatch_L2ToL3_Success() {
        String rewardBatchId = "BATCH4";

        when(rewardBatchService.validateRewardBatch("operator2", INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.empty());

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/validated",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-organization-role", "operator2")
                .exchange()
                .expectStatus().isOk();

        verify(rewardBatchService, times(1)).validateRewardBatch("operator2", INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void validateRewardBatch_InvalidState() {
        String rewardBatchId = "BATCH5";

        when(rewardBatchService.validateRewardBatch("operator3", INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                        ExceptionConstants.ExceptionCode.INVALID_BATCH_STATE_FOR_PROMOTION)));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/validated",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-organization-role", "operator3")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo(ExceptionConstants.ExceptionCode.INVALID_BATCH_STATE_FOR_PROMOTION);

        verify(rewardBatchService, times(1)).validateRewardBatch("operator3", INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void validateRewardBatch_NotFound() {
        String rewardBatchId = "BATCH6";

        when(rewardBatchService.validateRewardBatch("operator1", INITIATIVE_ID, rewardBatchId))
                .thenReturn(Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND)));

        webClient.post()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/validated",
                        INITIATIVE_ID, rewardBatchId)
                .header("x-organization-role", "operator1")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message").isEqualTo(ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND);

        verify(rewardBatchService, times(1)).validateRewardBatch("operator1", INITIATIVE_ID, rewardBatchId);
    }

    @Test
    void downloadApprovedRewardBatch_Success() {
        DownloadRewardBatchResponseDTO responseDTO = DownloadRewardBatchResponseDTO.builder()
                .approvedBatchUrl("https://blobstorage/signed-url")
                .build();

        when(rewardBatchService.downloadApprovedRewardBatchFile(
                MERCHANT_ID,
                "operator1",
                INITIATIVE_ID,
                REWARD_BATCH_ID_1
        )).thenReturn(Mono.just(responseDTO));

        webClient.get()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved/download",
                        INITIATIVE_ID, REWARD_BATCH_ID_1)
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-organization-role", "operator1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(DownloadRewardBatchResponseDTO.class)
                .value(dto -> {
                    assertNotNull(dto);
                    assertEquals("https://blobstorage/signed-url", dto.getApprovedBatchUrl());
                });

        verify(rewardBatchService, times(1))
                .downloadApprovedRewardBatchFile(
                        MERCHANT_ID,
                        "operator1",
                        INITIATIVE_ID,
                        REWARD_BATCH_ID_1
                );
    }

    @Test
    void downloadApprovedRewardBatch_ServiceFails_BadRequest() {
        when(rewardBatchService.downloadApprovedRewardBatchFile(
                MERCHANT_ID,
                "operator1",
                INITIATIVE_ID,
                REWARD_BATCH_ID_1
        )).thenReturn(Mono.error(
                new RewardBatchException(
                        HttpStatus.BAD_REQUEST,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND
                )
        ));

        webClient.get()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved/download",
                        INITIATIVE_ID, REWARD_BATCH_ID_1)
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-organization-role", "operator1")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo(ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND);

        verify(rewardBatchService, times(1))
                .downloadApprovedRewardBatchFile(
                        MERCHANT_ID,
                        "operator1",
                        INITIATIVE_ID,
                        REWARD_BATCH_ID_1
                );
    }

  @Test
  void postponeTransaction_success() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.of(2026, 1, 6);

    when(rewardBatchService.postponeTransaction(
        MERCHANT_ID,
        INITIATIVE_ID,
        REWARD_BATCH_ID_1,
        transactionId,
        initiativeEndDate
    )).thenReturn(Mono.empty());

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/{transactionId}/postpone")
            .queryParam("initiativeEndDate", initiativeEndDate.toString())
            .build(INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isNoContent();

    verify(rewardBatchService, times(1))
        .postponeTransaction(MERCHANT_ID, INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId, initiativeEndDate);
  }

  @Test
  void postponeTransaction_transactionNotFound() {
    String transactionId = "TX_NOT_EXIST";
    LocalDate initiativeEndDate = LocalDate.now();

    when(rewardBatchService.postponeTransaction(
        anyString(), anyString(), anyString(), eq(transactionId), any(LocalDate.class)
    )).thenReturn(Mono.error(new ClientExceptionNoBody(HttpStatus.NOT_FOUND, ExceptionMessage.TRANSACTION_NOT_FOUND)));

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/{transactionId}/postpone")
            .queryParam("initiativeEndDate", initiativeEndDate.toString())
            .build(INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isNotFound();
  }

  @Test
  void postponeTransaction_batchNotFound() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.now();

    when(rewardBatchService.postponeTransaction(
        anyString(), anyString(), anyString(), eq(transactionId), any(LocalDate.class)
    )).thenReturn(Mono.error(new ClientExceptionWithBody(
        HttpStatus.NOT_FOUND, ExceptionCode.REWARD_BATCH_NOT_FOUND, String.format(ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH, REWARD_BATCH_ID_1))));

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/{transactionId}/postpone")
            .queryParam("initiativeEndDate", initiativeEndDate.toString())
            .build(INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isNotFound()
        .expectBody()
        .jsonPath("$.code").isEqualTo(ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND)
        .jsonPath("$.message").isEqualTo(String.format(ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH, REWARD_BATCH_ID_1));
  }

  @Test
  void postponeTransaction_batchInvalidStatus() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.now();

    when(rewardBatchService.postponeTransaction(
        anyString(), anyString(), anyString(), eq(transactionId), any(LocalDate.class)
    )).thenReturn(Mono.error(new ClientExceptionWithBody(
        HttpStatus.BAD_REQUEST, ExceptionCode.REWARD_BATCH_INVALID_REQUEST, ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH)));

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/{transactionId}/postpone")
            .queryParam("initiativeEndDate", initiativeEndDate.toString())
            .build(INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.code").isEqualTo(ExceptionCode.REWARD_BATCH_INVALID_REQUEST)
        .jsonPath("$.message").isEqualTo(ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH);
  }

  @Test
  void postponeTransaction_exceedsLimit() {
    String transactionId = "TX123";
    LocalDate initiativeEndDate = LocalDate.now();

    when(rewardBatchService.postponeTransaction(
        anyString(), anyString(), anyString(), eq(transactionId), any(LocalDate.class)
    )).thenReturn(Mono.error(new ClientExceptionWithBody(
        HttpStatus.BAD_REQUEST, ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED, ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED)));

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/{transactionId}/postpone")
            .queryParam("initiativeEndDate", initiativeEndDate.toString())
            .build(INITIATIVE_ID, REWARD_BATCH_ID_1, transactionId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.code").isEqualTo(ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED)
        .jsonPath("$.message").isEqualTo(ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED);
  }
}
