package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@DirtiesContext
class RewardTransactionSpecificRepositoryTest extends BaseIntegrationTest {
    @Autowired
    protected RewardTransactionRepository rewardTransactionRepository;

    @Autowired
    private RewardTransactionSpecificRepositoryImpl rewardTransactionSpecificRepository;

    private RewardTransaction rt;
    private RewardTransaction rt1;
    private RewardTransaction rt2;
    private RewardTransaction rt3;

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
        rewardTransactionRepository.deleteById("id_prova").block();
    }

    @Test
    void findByIdTrxIssuer() {
        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,null, null, null);
        Assertions.assertNotNull(resultTrxIssuer);
        List<RewardTransaction> rewardTransactionsList = resultTrxIssuer.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList.size());
        Assertions.assertEquals(rewardTransactionsList, List.of(rt));
    }

    @Test
    void findByIdTrxIssuerAndOptionalFilters() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),rt.getUserId() ,null,null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        List<RewardTransaction> resultTrxIssuerAndUserIdList = resultTrxIssuerAndUserId.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndUserIdList.size());
        Assertions.assertEquals(resultTrxIssuerAndUserIdList, List.of(rt));

        Flux<RewardTransaction> resultTrxIssuerAndStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,startDate,null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndStartDate);
        List<RewardTransaction> resultTrxIssuerAndStartDateList = resultTrxIssuerAndStartDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndStartDateList.size());
        Assertions.assertEquals(resultTrxIssuerAndStartDateList, List.of(rt));

        Flux<RewardTransaction> resultTrxIssuerAndEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,endDate, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndEndDate);
        List<RewardTransaction> resultTrxIssuerAndEndDateList = resultTrxIssuerAndEndDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndEndDateList.size());
        Assertions.assertEquals(resultTrxIssuerAndEndDateList, List.of(rt));


        Flux<RewardTransaction> resultTrxIssuerAndAmount = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,null, rt.getAmount(), null);
        Assertions.assertNotNull(resultTrxIssuerAndAmount);
        List<RewardTransaction> resultTrxIssuerAndAmountList = resultTrxIssuerAndAmount.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndAmountList.size());
        Assertions.assertEquals(resultTrxIssuerAndAmountList, List.of(rt));

        Flux<RewardTransaction> resultTrxIssuerAndRangeDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,startDate,endDate, rt.getAmount(), null);
        Assertions.assertNotNull(resultTrxIssuerAndRangeDate);
        List<RewardTransaction> resultTrxIssuerAndRangeDateList = resultTrxIssuerAndRangeDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndRangeDateList.size());
        Assertions.assertEquals(resultTrxIssuerAndRangeDateList, List.of(rt));

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
        List<RewardTransaction> resultUserIDAndRangeDateList = resultUserIDAndRangeDate.toStream().toList();
        Assertions.assertEquals(1, resultUserIDAndRangeDateList.size());
        Assertions.assertEquals(resultUserIDAndRangeDateList, List.of(rt));

        Flux<RewardTransaction> resultUserIDAndRangeDateAndAmount = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,endDate,rt.getAmount(), null);
        Assertions.assertNotNull(resultUserIDAndRangeDateAndAmount);
        List<RewardTransaction> resultUserIDAndRangeDateAndAmountList = resultUserIDAndRangeDateAndAmount.toStream().toList();
        Assertions.assertEquals(1, resultUserIDAndRangeDateAndAmountList.size());
        Assertions.assertEquals(resultUserIDAndRangeDateAndAmountList, List.of(rt));

        Flux<RewardTransaction> resultUserIDBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), rt.getTrxDate().plusDays(10L) ,endDate,null, null);
        Assertions.assertNotNull(resultUserIDBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultUserIDBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultUserIDDateAfterEndDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,rt.getTrxDate().minusDays(10L),null, null);
        Assertions.assertNotNull(resultUserIDDateAfterEndDate);
        Assertions.assertEquals(0, resultUserIDDateAfterEndDate.count().block());
    }

    @Test
    void pageableWithfindByIdTrxIssuer(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        setUpPageable(date, "userId");

        Pageable pageable = PageRequest.of(0,2);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList.size());
        Assertions.assertEquals(rewardTransactionsList, List.of(rt1, rt2));

        Pageable pageable2 = PageRequest.of(1,2);
        Flux<RewardTransaction> result2 = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable2);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList2.size());
        Assertions.assertEquals(rewardTransactionsList2, List.of(rt3));

        Pageable pageable3 = PageRequest.of(0,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable3);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList3.size());
        Assertions.assertEquals(rewardTransactionsList3, List.of(rt3, rt2));

        Pageable pageable4 = PageRequest.of(1,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable4);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList4.size());
        Assertions.assertEquals(rewardTransactionsList4, List.of(rt1));

        cleanDataPageable();
    }

    @Test
    void pageableWithfindByRange(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        LocalDateTime startDate = date.minusDays(10L);
        LocalDateTime endDate = date.plusDays(10L);
        String userId = "USERID";

        setUpPageable(date, userId);

        Pageable pageable = PageRequest.of(0,2);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList.size());
        Assertions.assertEquals(rewardTransactionsList, List.of(rt1, rt2));

        Pageable pageable2 = PageRequest.of(1,2);
        Flux<RewardTransaction> result2 = rewardTransactionSpecificRepository.findByRange( userId,startDate,endDate, null, pageable2);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList2.size());
        Assertions.assertEquals(rewardTransactionsList2, List.of(rt3));

        Pageable pageable3 = PageRequest.of(0,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 = rewardTransactionSpecificRepository.findByRange(userId,startDate, endDate, null, pageable3);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList3.size());
        Assertions.assertEquals(rewardTransactionsList3, List.of(rt3, rt2));

        Pageable pageable4 = PageRequest.of(1,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 = rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable4);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList4.size());
        Assertions.assertEquals(rewardTransactionsList4, List.of(rt1));

        cleanDataPageable();
    }

    void setUpPageable(LocalDateTime date, String userId){
        BigDecimal amount = new BigDecimal("30.00");
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("id3")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt3).block();
    }

    void cleanDataPageable(){
        rewardTransactionRepository.deleteById("id1").block();
        rewardTransactionRepository.deleteById("id2").block();
        rewardTransactionRepository.deleteById("id3").block();
    }
}