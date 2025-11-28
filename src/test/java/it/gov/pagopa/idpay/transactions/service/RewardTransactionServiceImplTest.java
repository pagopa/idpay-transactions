package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleTypeEnum;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@ExtendWith(MockitoExtension.class)
class RewardTransactionServiceImplTest {
    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Mock
    private RewardBatchService rewardBatchService;

    @Mock
    private MerchantRestClient merchantRestClient;

    private final int seed = 15121984;

    private RewardTransactionService rewardTransactionService;
    @BeforeEach
    void setUp(){
        rewardTransactionService = new RewardTransactionServiceImpl(
            rewardTransactionRepository,
            rewardBatchService,
            merchantRestClient,
            seed
        );
    }

    @Test
    void findByIdTrxIssuer() {
        // Given
        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amountCents(3000L)
                .trxDate(LocalDateTime.of(2022, 9, 19, 15,43,39))
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, null, null, null, null)).thenReturn(Flux.just(rt));

        // When
        Flux<RewardTransaction> result = rewardTransactionService.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, null);
        Assertions.assertNotNull(result);
        RewardTransaction resultRT = result.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(resultRT);
        Assertions.assertEquals(rt, resultRT);
    }

    @Test
    void findByRange() {
        // Given
        LocalDateTime date = LocalDateTime.of(2022, 9, 19, 15,43,39);
        LocalDateTime startDate = date.minusMonths(9L);
        LocalDateTime endDate = date.plusMonths(6L);


        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amountCents(3000L)
                .trxDate(date)
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.findByRange(rt.getUserId(), startDate, endDate, null, null)).thenReturn(Flux.just(rt));

        // When
        Flux<RewardTransaction> result = rewardTransactionService.findByRange(rt.getUserId(), startDate, endDate, null, null);
        Assertions.assertNotNull(result);
        RewardTransaction resultRT = result.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(resultRT);
        Assertions.assertEquals(rt, resultRT);
    }

    @Test
    void save(){
        // Given
        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amountCents(3000L)
                .trxDate(LocalDateTime.of(2022, 9, 19, 15,43,39))
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.save(rt)).thenReturn(Mono.just(rt));

        //when
        RewardTransaction result = rewardTransactionService.save(rt).block();

        //Then
        Assertions.assertNotNull(result);
        Assertions.assertEquals(rt, result);
        Mockito.verifyNoMoreInteractions(rewardTransactionRepository);
    }

    @Test
    void save_invoiced_enrichesBatch() {
        RewardTransaction rt = RewardTransaction.builder()
            .id("TRX1")
            .userId("USERID")
            .amountCents(3000L)
            .trxDate(LocalDateTime.of(2022, 9, 19, 15, 43, 39))
            .idTrxIssuer("IDTRXISSUER")
            .status("INVOICED")
            .merchantId("MERCHANT1")
            .pointOfSaleType(PosType.ONLINE)
            .pointOfSaleId("POS1")
            .businessName("Test Business")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39)) // <--- aggiunto
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(1000L).build()))
            .initiatives(List.of("initiative1"))
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardBatchService.findOrCreateBatch(
            rt.getMerchantId(),
            rt.getPointOfSaleType(),
            "2025-11",
            rt.getBusinessName()
        )).thenReturn(Mono.just(batch));

        Mockito.when(rewardBatchService.incrementTotals(batch.getId(), 1000L))
            .thenReturn(Mono.just(batch));

        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        RewardTransaction result = rewardTransactionService.save(rt).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals("BATCH1", result.getRewardBatchId());
        Assertions.assertEquals(RewardBatchTrxStatus.CONSULTABLE, result.getRewardBatchTrxStatus());
        Assertions.assertNotNull(result.getRewardBatchInclusionDate());
        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).save(Mockito.any());
    }

    @Test
    void assignInvoicedTransactionsToBatches_processAllProcessesAllTransactions() {
        int chunkSize = 200;

        RewardTransaction trx1 = RewardTransaction.builder()
            .id("TRX1")
            .userId("USER1")
            .amountCents(1000L)
            .status("INVOICED")
            .merchantId("M1")
            .pointOfSaleId("POS1")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .initiatives(List.of("initiative1"))
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(500L).build()))
            .build();

        RewardTransaction trx2 = RewardTransaction.builder()
            .id("TRX2")
            .userId("USER2")
            .amountCents(2000L)
            .status("INVOICED")
            .merchantId("M2")
            .pointOfSaleId("POS2")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .initiatives(List.of("initiative2"))
            .rewards(Map.of("initiative2", Reward.builder().accruedRewardCents(1000L).build()))
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(chunkSize))
            .thenReturn(Flux.just(trx1, trx2))
            .thenReturn(Flux.empty());

        Mockito.when(merchantRestClient.getPointOfSale(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(Mono.just(PointOfSaleDTO.builder()
                .type(PointOfSaleTypeEnum.ONLINE)
                .franchiseName("FranchiseName")
                .businessName("BusinessName")
                .build()));

        Mockito.when(rewardBatchService.findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardBatchService.incrementTotals(Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(Mono.just(batch));

        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        Mono<Void> result = rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize, true);
        result.block();

        Mockito.verify(rewardTransactionRepository, Mockito.times(2))
            .findInvoicedTransactionsWithoutBatch(chunkSize);

        Mockito.verify(merchantRestClient, Mockito.times(2))
            .getPointOfSale(Mockito.anyString(), Mockito.anyString());

        Mockito.verify(rewardBatchService, Mockito.times(2))
            .findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any());
        Mockito.verify(rewardBatchService, Mockito.times(2))
            .incrementTotals(Mockito.anyString(), Mockito.anyLong());

        Mockito.verify(rewardTransactionRepository, Mockito.times(2))
            .save(Mockito.any());
    }

    @Test
    void assignInvoicedTransactionsToBatches_singleOperation_processesTransactions() {
        int chunkSize = 20;
        boolean processAll = false;

        RewardTransaction trx1 = RewardTransaction.builder()
            .id("TRX1")
            .userId("USER1")
            .amountCents(1000L)
            .status("INVOICED")
            .merchantId("M1")
            .pointOfSaleId("POS1")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .initiatives(List.of("initiative1"))
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(500L).build()))
            .build();

        RewardTransaction trx2 = RewardTransaction.builder()
            .id("TRX2")
            .userId("USER2")
            .amountCents(2000L)
            .status("INVOICED")
            .merchantId("M2")
            .pointOfSaleId("POS2")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .initiatives(List.of("initiative2"))
            .rewards(Map.of("initiative2", Reward.builder().accruedRewardCents(1000L).build()))
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(chunkSize))
            .thenReturn(Flux.just(trx1, trx2));

        Mockito.when(merchantRestClient.getPointOfSale(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(Mono.just(PointOfSaleDTO.builder()
                .type(PointOfSaleTypeEnum.ONLINE)
                .franchiseName("FranchiseName")
                .businessName("BusinessName")
                .build()));

        Mockito.when(rewardBatchService.findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardBatchService.incrementTotals(Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(Mono.just(batch));

        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        Mono<Void> result = rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize, processAll);

        result.block();

        Mockito.verify(rewardTransactionRepository, Mockito.times(1))
            .findInvoicedTransactionsWithoutBatch(chunkSize);

        Mockito.verify(merchantRestClient, Mockito.times(2))
            .getPointOfSale(Mockito.anyString(), Mockito.anyString());

        Mockito.verify(rewardBatchService, Mockito.times(2))
            .findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any());
        Mockito.verify(rewardBatchService, Mockito.times(2))
            .incrementTotals(Mockito.anyString(), Mockito.anyLong());

        Mockito.verify(rewardTransactionRepository, Mockito.times(2))
            .save(Mockito.any());
    }

    @Test
    void assignInvoicedTransactionsToBatches_singleOperation_noTransactions() {
        int chunkSize = 200;
        boolean processAll = false;

        Mockito.when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(chunkSize))
            .thenReturn(Flux.empty());

        Mono<Void> result = rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize, processAll);

        result.block();

        Mockito.verify(rewardTransactionRepository, Mockito.times(1))
            .findInvoicedTransactionsWithoutBatch(chunkSize);

        Mockito.verifyNoInteractions(merchantRestClient, rewardBatchService);
    }

    @Test
    void assignInvoicedTransactionsToBatches_enrichesMissingFields() {
        int chunkSize = 100;

        RewardTransaction trx = RewardTransaction.builder()
            .id("TRX1")
            .status("INVOICED")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 10, 0))
            .invoiceUploadDate(null)
            .franchiseName(null)
            .pointOfSaleType(null)
            .businessName(null)
            .initiatives(List.of("initiative1"))
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(100L).build()))
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(chunkSize))
            .thenReturn(Flux.just(trx))
            .thenReturn(Flux.empty());

        Mockito.when(merchantRestClient.getPointOfSale(Mockito.any(), Mockito.any()))
            .thenReturn(Mono.just(PointOfSaleDTO.builder()
                .type(PointOfSaleTypeEnum.ONLINE)
                .franchiseName("FranchiseName")
                .businessName("BusinessName")
                .build()));

        Mockito.when(rewardBatchService.findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardBatchService.incrementTotals(Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize, false).block();

        Mockito.verify(rewardTransactionRepository).save(Mockito.argThat(saved ->
            saved.getInvoiceUploadDate().equals(trx.getTrxChargeDate()) &&
                "FranchiseName".equals(saved.getFranchiseName()) &&
                saved.getPointOfSaleType() == PosType.ONLINE &&
                "BusinessName".equals(saved.getBusinessName())
        ));
    }

    @Test
    void assignInvoicedTransactionsToBatches_noEnrichmentNeeded() {
        int chunkSize = 100;

        RewardTransaction trx = RewardTransaction.builder()
            .id("TRX2")
            .status("INVOICED")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 10, 0))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 10, 0))
            .franchiseName("FranchiseName")
            .pointOfSaleType(PosType.ONLINE)
            .businessName("BusinessName")
            .initiatives(List.of("initiative1"))
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(100L).build()))
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(chunkSize))
            .thenReturn(Flux.just(trx))
            .thenReturn(Flux.empty());

        Mockito.when(merchantRestClient.getPointOfSale(Mockito.any(), Mockito.any()))
            .thenReturn(Mono.just(PointOfSaleDTO.builder()
                .type(PointOfSaleTypeEnum.ONLINE)
                .franchiseName("FranchiseName")
                .businessName("BusinessName")
                .build()));

        Mockito.when(rewardBatchService.findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardBatchService.incrementTotals(Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize, false).block();

        Mockito.verify(rewardTransactionRepository).save(Mockito.argThat(saved ->
            saved.getInvoiceUploadDate().equals(trx.getInvoiceUploadDate()) &&
                "FranchiseName".equals(saved.getFranchiseName()) &&
                saved.getPointOfSaleType() == PosType.ONLINE &&
                "BusinessName".equals(saved.getBusinessName())
        ));
    }

    @Test
    void assignInvoicedTransactionsToBatches_partialEnrichment() {
        int chunkSize = 100;

        RewardTransaction trx = RewardTransaction.builder()
            .id("TRX3")
            .status("INVOICED")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 10, 0))
            .invoiceUploadDate(LocalDateTime.of(2025, 11, 19, 10, 0))
            .franchiseName(null)
            .pointOfSaleType(null)
            .businessName(null)
            .initiatives(List.of("initiative1"))
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(100L).build()))
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(chunkSize))
            .thenReturn(Flux.just(trx))
            .thenReturn(Flux.empty());

        Mockito.when(merchantRestClient.getPointOfSale(Mockito.any(), Mockito.any()))
            .thenReturn(Mono.just(PointOfSaleDTO.builder()
                .type(PointOfSaleTypeEnum.ONLINE)
                .franchiseName("FranchiseName")
                .businessName("BusinessName")
                .build()));

        Mockito.when(rewardBatchService.findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardBatchService.incrementTotals(Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(Mono.just(batch));
        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        rewardTransactionService.assignInvoicedTransactionsToBatches(chunkSize, false).block();

        Mockito.verify(rewardTransactionRepository).save(Mockito.argThat(saved ->
            saved.getInvoiceUploadDate().equals(trx.getInvoiceUploadDate()) &&
                "FranchiseName".equals(saved.getFranchiseName()) &&
                saved.getPointOfSaleType() == PosType.ONLINE &&
                "BusinessName".equals(saved.getBusinessName())
        ));
    }
}
