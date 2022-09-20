package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.exception.ClientExceptionWithBody;
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
        rewardTransactionRepository.deleteById("id_prova");
    }

    @Test
    void findTrxs(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        LocalDateTime startDate = date.minusMonths(5L);
        LocalDateTime endDate = date.plusMonths(6L);
        BigDecimal amount = new BigDecimal("30.00");
        RewardTransaction rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id_prova")
                .trxDate(date)
                .amount(amount).build();
        rewardTransactionRepository.save(rt).block();

        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionRepository.findByFilters("IDTRXISSUER1",null ,null,startDate,endDate);
        Assertions.assertNotNull(resultTrxIssuer);
        Assertions.assertEquals(1, resultTrxIssuer.count().block());

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionRepository.findByFilters("IDTRXISSUER1","USERID1",null,startDate, endDate);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        Assertions.assertEquals(1, resultTrxIssuerAndUserId.count().block());

        Flux<RewardTransaction> resultTrxIssuerAndUserIdAndAmount = rewardTransactionRepository.findByFilters("IDTRXISSUER1","USERID1",new BigDecimal("30.00"),startDate, endDate);
        Assertions.assertNotNull(resultTrxIssuerAndUserIdAndAmount);
        Assertions.assertEquals(1, resultTrxIssuerAndUserIdAndAmount.count().block());

        Flux<RewardTransaction> resultNotTrxIssuer = rewardTransactionRepository.findByFilters(null,"USERID1", amount, startDate, endDate);
        Assertions.assertNotNull(resultNotTrxIssuer);
        Assertions.assertEquals(1, resultNotTrxIssuer.count().block());

        Flux.defer(() -> rewardTransactionRepository.findByFilters(null,"USERID1", null, startDate, endDate))
                .onErrorContinue((t, o) -> Assertions.assertTrue(t instanceof ClientExceptionWithBody));

        Flux.defer(() -> rewardTransactionRepository.findByFilters(null,null, null, startDate, endDate))
                .onErrorContinue((t, o) -> Assertions.assertTrue(t instanceof ClientExceptionWithBody));

    }
}