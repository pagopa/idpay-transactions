package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;

class RewardTransactionRepositoryTest extends BaseIntegrationTest {

    @Autowired
    protected RewardTransactionRepository rewardTransactionRepository;

    @AfterEach
    void clearData(){
        rewardTransactionRepository.deleteAll().block();
    }

    @Test
    void findBetweenDate() {
        RewardTransaction rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .trxDate(LocalDateTime.now().minusYears(5L)).build();
        RewardTransaction rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .trxDate(LocalDateTime.of(2021, 04, 06, 17, 30, 25)).build();
        RewardTransaction rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .trxDate(LocalDateTime.now()).build();
        RewardTransaction rt4 = RewardTransactionFaker.mockInstanceBuilder(4)
                .trxDate(LocalDateTime.now().plusYears(7L)).build();

        rewardTransactionRepository.save(rt1).block();
        rewardTransactionRepository.save(rt2).block();
        rewardTransactionRepository.save(rt3).block();
        rewardTransactionRepository.save(rt4).block();

        LocalDateTime startSearch = LocalDateTime.now().minusYears(2L);
        LocalDateTime endSearch = LocalDateTime.now().plusMonths(7);

        Flux<RewardTransaction> betweenDateResult = rewardTransactionRepository.findAllInRange(startSearch, endSearch);
        betweenDateResult.toIterable().forEach(rt -> {
            Assertions.assertNotNull(rt.getTrxDate());
            Assertions.assertFalse(rt.getTrxDate().isBefore(startSearch));
            Assertions.assertFalse(rt.getTrxDate().isAfter(endSearch));
        });


    }
}