package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@DirtiesContext
class RewardTransactionSpecificRepositoryTest extends BaseIntegrationTest {
    //TODO pageable add null

    @Autowired
    protected RewardTransactionRepository rewardTransactionRepository;

    @Autowired
    private RewardTransactionSpecificRepositoryImpl rewardTransactionSpecificRepository;

    private RewardTransaction rt;

    @BeforeEach
    void setUp(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        BigDecimal amount = new BigDecimal("30.00");
        rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id_prova")
                .idTrxIssuer("IDTRXISSUER1")
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt).block();
    }

    @AfterEach
    void clearData(){
        rewardTransactionRepository.deleteById("id_prova");
    }

    @Test
    void findByIdTrxIssuer() {
        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,null, null, null);
        Assertions.assertNotNull(resultTrxIssuer);
        Assertions.assertEquals(1, resultTrxIssuer.count().block());
        RewardTransaction rtResultTrxIssuer = resultTrxIssuer.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuer);
        Assertions.assertEquals(rt, rtResultTrxIssuer);
    }

    @Test
    void findByIdTrxIssuerAndOptionalFilters() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),rt.getUserId() ,null,null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        Assertions.assertEquals(1, resultTrxIssuerAndUserId.count().block());
        RewardTransaction rtResultTrxIssuerAndUserId = resultTrxIssuerAndUserId.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndUserId);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndUserId);

        Flux<RewardTransaction> resultTrxIssuerAndStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,startDate,null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndStartDate);
        Assertions.assertEquals(1, resultTrxIssuerAndStartDate.count().block());
        RewardTransaction rtResultTrxIssuerAndStartDate = resultTrxIssuerAndStartDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndStartDate);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndStartDate);

        Flux<RewardTransaction> resultTrxIssuerAndEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,endDate, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndEndDate);
        Assertions.assertEquals(1, resultTrxIssuerAndEndDate.count().block());
        RewardTransaction rtResultTrxIssuerAndEndDate = resultTrxIssuerAndEndDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndEndDate);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndEndDate);

        Flux<RewardTransaction> resultTrxIssuerAndAmount = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,null, rt.getAmount(), null);
        Assertions.assertNotNull(resultTrxIssuerAndAmount);
        Assertions.assertEquals(1, resultTrxIssuerAndAmount.count().block());
        RewardTransaction rtResultTrxIssuerAndAmount = resultTrxIssuerAndAmount.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndAmount);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndAmount);

        Flux<RewardTransaction> resultTrxIssuerAndRangeDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,startDate,endDate, rt.getAmount(), null);
        Assertions.assertNotNull(resultTrxIssuerAndRangeDate);
        Assertions.assertEquals(1, resultTrxIssuerAndRangeDate.count().block());
        RewardTransaction rtResultTrxIssuerAndRangeDate = resultTrxIssuerAndRangeDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultTrxIssuerAndRangeDate);
        Assertions.assertEquals(rt, rtResultTrxIssuerAndRangeDate);

        Flux<RewardTransaction> resultBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,rt.getTrxDate().plusDays(10L),null, null, null);
        Assertions.assertNotNull(resultBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultDateAfterEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,rt.getTrxDate().minusDays(10L), null, null);
        Assertions.assertNotNull(resultDateAfterEndDate);
        Assertions.assertEquals(0, resultDateAfterEndDate.count().block());
    }
    @Test
    void findByUserIdAndRangeDateAndAmount() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultUserIDAndRangeDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,endDate,null, null);
        Assertions.assertNotNull(resultUserIDAndRangeDate);
        Assertions.assertEquals(1, resultUserIDAndRangeDate.count().block());
        RewardTransaction rtResultUserIDAndRangeDate = resultUserIDAndRangeDate.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultUserIDAndRangeDate);
        Assertions.assertEquals(rt, rtResultUserIDAndRangeDate);

        Flux<RewardTransaction> resultUserIDAndRangeDateAndAmount = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,endDate,rt.getAmount(), null);
        Assertions.assertNotNull(resultUserIDAndRangeDateAndAmount);
        Assertions.assertEquals(1, resultUserIDAndRangeDateAndAmount.count().block());
        RewardTransaction rtResultUserIDAndRangeDateAndAmount = resultUserIDAndRangeDateAndAmount.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultUserIDAndRangeDateAndAmount);
        Assertions.assertEquals(rt, rtResultUserIDAndRangeDateAndAmount);

        Flux<RewardTransaction> resultUserIDBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), rt.getTrxDate().plusDays(10L) ,endDate,null, null);
        Assertions.assertNotNull(resultUserIDBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultUserIDBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultUserIDDateAfterEndDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,rt.getTrxDate().minusDays(10L),null, null);
        Assertions.assertNotNull(resultUserIDDateAfterEndDate);
        Assertions.assertEquals(0, resultUserIDDateAfterEndDate.count().block());
    }
}