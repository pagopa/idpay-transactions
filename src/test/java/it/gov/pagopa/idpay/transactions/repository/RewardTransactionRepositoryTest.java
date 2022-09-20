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
class RewardTransactionRepositoryTest extends BaseIntegrationTest {

    @Autowired
    protected RewardTransactionRepository rewardTransactionRepository;

    @AfterEach
    void clearData(){
        rewardTransactionRepository.deleteAll().block();
    }

    @Test
    void findTrxs(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        BigDecimal amount = new BigDecimal("30.00");
        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt1).block();
        RewardTransaction rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .trxDate(LocalDateTime.of(2021, 4, 6, 17, 30, 25)).build();
        rewardTransactionRepository.save(rt2).block();

        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionRepository.findByFilters("IDTRXISSUER1",null ,null,null);
        Assertions.assertNotNull(resultTrxIssuer);
        Assertions.assertEquals(1, resultTrxIssuer.count().block());

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionRepository.findByFilters("IDTRXISSUER1","USERID1",null,null);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        Assertions.assertEquals(1, resultTrxIssuerAndUserId.count().block());

        Flux<RewardTransaction> resultTrxIssuerAndAnotherUserId = rewardTransactionRepository.findByFilters("IDTRXISSUER1","USERID2" ,null,null);
        Assertions.assertNotNull(resultTrxIssuerAndAnotherUserId);
        Assertions.assertEquals(0, resultTrxIssuerAndAnotherUserId.count().block());

        Flux<RewardTransaction> resultNotTrxIssuer = rewardTransactionRepository.findByFilters(null,"USERID1",date, amount);
        Assertions.assertNotNull(resultNotTrxIssuer);
        Assertions.assertEquals(1, resultNotTrxIssuer.count().block());
    }
}