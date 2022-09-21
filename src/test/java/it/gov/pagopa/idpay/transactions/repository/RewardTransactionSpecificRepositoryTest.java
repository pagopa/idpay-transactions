package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@DirtiesContext
class RewardTransactionSpecificRepositoryTest extends BaseIntegrationTest {

    @Autowired
    protected RewardTransactionRepository rewardTransactionRepository;

    @Autowired
    private RewardTransactionSpecificRepositoryImpl rewardTransactionSpecificRepository;

    @AfterEach
    void clearData(){
        rewardTransactionRepository.deleteById("id_prova");
    }

    @Test
    void findByIdTrxIssuerAndOtherFilters() {
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        LocalDateTime startDate = date.minusMonths(5L);
        LocalDateTime endDate = date.plusMonths(6L);
        BigDecimal amount = new BigDecimal("30.00");
        RewardTransaction rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id_prova")
                .idTrxIssuer("IDTRXISSUER1")
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt).block();

        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),null ,null,null, null);
        Assertions.assertNotNull(resultTrxIssuer);
        Assertions.assertEquals(1, resultTrxIssuer.count().block());
        RewardTransaction rtResultTrxIssuer = resultTrxIssuer.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuer);
        Assertions.assertEquals(rt, rtResultTrxIssuer);

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),rt.getUserId() ,null,null, null);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        Assertions.assertEquals(1, resultTrxIssuerAndUserId.count().block());
        RewardTransaction rtResultTrxIssuerAndUserId = resultTrxIssuerAndUserId.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndUserId);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndUserId);

        Flux<RewardTransaction> resultTrxIssuerAndStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),null ,startDate,null, null);
        Assertions.assertNotNull(resultTrxIssuerAndStartDate);
        Assertions.assertEquals(1, resultTrxIssuerAndStartDate.count().block());
        RewardTransaction rtResultTrxIssuerAndStartDate = resultTrxIssuerAndStartDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndStartDate);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndStartDate);

        Flux<RewardTransaction> resultTrxIssuerAndEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),null ,null,endDate, null);
        Assertions.assertNotNull(resultTrxIssuerAndEndDate);
        Assertions.assertEquals(1, resultTrxIssuerAndEndDate.count().block());
        RewardTransaction rtResultTrxIssuerAndEndDate = resultTrxIssuerAndEndDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndEndDate);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndEndDate);

        Flux<RewardTransaction> resultTrxIssuerAndAmount = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),null ,null,null, rt.getAmount());
        Assertions.assertNotNull(resultTrxIssuerAndAmount);
        Assertions.assertEquals(1, resultTrxIssuerAndAmount.count().block());
        RewardTransaction rtResultTrxIssuerAndAmount = resultTrxIssuerAndAmount.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndAmount);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndAmount);

        Flux<RewardTransaction> resultBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),null ,date.plusDays(10L),null, null);
        Assertions.assertNotNull(resultBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultDateAfterEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(),null ,null,date.minusDays(10L), null);
        Assertions.assertNotNull(resultDateAfterEndDate);
        Assertions.assertEquals(0, resultDateAfterEndDate.count().block());
    }

    @Test
    void findByUserIdAndRangeDateAndAmount() {
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        LocalDateTime startDate = date.minusMonths(5L);
        LocalDateTime endDate = date.plusMonths(6L);
        BigDecimal amount = new BigDecimal("30.00");
        RewardTransaction rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id_prova")
                .idTrxIssuer("IDTRXISSUER1")
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt).block();

        Flux<RewardTransaction> resultUserIDAndRangeDate = rewardTransactionSpecificRepository.findByUserIdAndRangeDateAndAmount(rt.getUserId(), startDate ,endDate,null);
        Assertions.assertNotNull(resultUserIDAndRangeDate);
        Assertions.assertEquals(1, resultUserIDAndRangeDate.count().block());
        RewardTransaction rtResultUserIDAndRangeDate = resultUserIDAndRangeDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultUserIDAndRangeDate);
        Assertions.assertEquals(rt, rtResultUserIDAndRangeDate);

        Flux<RewardTransaction> resultUserIDAndRangeDateAndAmount = rewardTransactionSpecificRepository.findByUserIdAndRangeDateAndAmount(rt.getUserId(), startDate ,endDate,null);
        Assertions.assertNotNull(resultUserIDAndRangeDateAndAmount);
        Assertions.assertEquals(1, resultUserIDAndRangeDateAndAmount.count().block());
        RewardTransaction rtResultUserIDAndRangeDateAndAmount = resultUserIDAndRangeDateAndAmount.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultUserIDAndRangeDateAndAmount);
        Assertions.assertEquals(rt, rtResultUserIDAndRangeDateAndAmount);

        Flux<RewardTransaction> resultUserIDBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByUserIdAndRangeDateAndAmount(rt.getUserId(), date.plusDays(10L) ,endDate,null);
        Assertions.assertNotNull(resultUserIDBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultUserIDBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultUserIDDateAfterEndDate = rewardTransactionSpecificRepository.findByUserIdAndRangeDateAndAmount(rt.getUserId(), startDate ,date.minusDays(10L),null);
        Assertions.assertNotNull(resultUserIDDateAfterEndDate);
        Assertions.assertEquals(0, resultUserIDDateAfterEndDate.count().block());
    }
}