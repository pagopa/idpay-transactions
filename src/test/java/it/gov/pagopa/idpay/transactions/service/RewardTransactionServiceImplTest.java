package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleTypeEnum;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

    private RewardTransactionService rewardTransactionService;
    @BeforeEach
    void setUp(){
        rewardTransactionService = new RewardTransactionServiceImpl(rewardTransactionRepository, rewardBatchService);
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

    @Disabled
    @Test
    void save_invoiced_enrichesBatch() {
        RewardTransaction rt = RewardTransaction.builder()
            .userId("USERID")
            .amountCents(3000L)
            .trxDate(LocalDateTime.of(2022, 9, 19, 15, 43, 39))
            .idTrxIssuer("IDTRXISSUER")
            .status("INVOICED")
            .merchantId("MERCHANT1")
            .pointOfSaleId("POS1")
            .trxChargeDate(LocalDateTime.of(2022, 9, 19, 15, 43, 39))
            .build();


        PointOfSaleDTO posDTO = new PointOfSaleDTO();
        posDTO.setId("POS1");
        posDTO.setType(PointOfSaleTypeEnum.ONLINE);


        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");
        Mockito.when(rewardBatchService.findOrCreateBatch(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
            .thenReturn(Mono.just(batch));

        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        RewardTransaction result = rewardTransactionService.save(rt).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals("BATCH1", result.getRewardBatchId());
        Assertions.assertEquals(RewardBatchTrxStatus.TO_CHECK, result.getRewardBatchTrxStatus());
        Assertions.assertNotNull(result.getRewardBatchInclusionDate());
        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).save(Mockito.any());
    }

}
