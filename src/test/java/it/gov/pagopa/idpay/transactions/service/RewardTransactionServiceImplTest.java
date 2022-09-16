package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

class RewardTransactionServiceImplTest {

    @Test
    void findAll() {
        // Given
        RewardTransactionRepository rewardTransactionRepository = Mockito.mock(RewardTransactionRepository.class);
        RewardTransactionMapper rewardTransactionMapper = Mockito.mock(RewardTransactionMapper.class);
        ErrorNotifierService errorNotifierService = Mockito.mock(ErrorNotifierService.class);
        ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);

        RewardTransactionService rewardTransactionService= new RewardTransactionServiceImpl(rewardTransactionRepository, rewardTransactionMapper,errorNotifierService, objectMapper);

        LocalDateTime start = LocalDateTime.now().minusYears(2L);
        LocalDateTime end = LocalDateTime.now();
        String userId = "USERID";
        String hpan = "HPAN";
        String acquuirerId = " ACQUIRERID";

        RewardTransaction transaction1 = RewardTransaction.builder().
                trxDate(end.minusMonths(3L))
                .userId(userId)
                .hpan(hpan)
                .acquirerId(acquuirerId).build();
        RewardTransaction transaction2 = RewardTransaction.builder().
                trxDate(end.minusMonths(4L))
                .userId("ANOTHER_USERID")
                .hpan(hpan)
                .acquirerId(acquuirerId).build();
        RewardTransaction transaction3 = RewardTransaction.builder().
                trxDate(end.minusMonths(2L))
                .userId(userId)
                .hpan("ANOTHER_HPAN")
                .acquirerId(acquuirerId).build();

        Mockito.when(rewardTransactionRepository.findAllInRange(start,end)).thenReturn(Flux.just(transaction1,transaction2,transaction3));
        // When
        Flux<RewardTransaction> resultAllFilters = rewardTransactionService.findAll(start.format(DateTimeFormatter.ISO_DATE_TIME), end.format(DateTimeFormatter.ISO_DATE_TIME), userId, hpan, acquuirerId);

        Flux<RewardTransaction> resultWithoutMandatoryField = rewardTransactionService.findAll(null, end.format(DateTimeFormatter.ISO_DATE_TIME), userId, hpan, acquuirerId);

        Flux<RewardTransaction> resultNotUserIdFilter = rewardTransactionService.findAll(start.format(DateTimeFormatter.ISO_DATE_TIME), end.format(DateTimeFormatter.ISO_DATE_TIME), null, hpan, acquuirerId);
        // Then
        Assertions.assertNotNull(resultAllFilters);
        Assertions.assertEquals(1, resultAllFilters.count().block());
        RewardTransaction rewardTransactionResultAllFilters = resultAllFilters.blockFirst();
        Assertions.assertNotNull(rewardTransactionResultAllFilters);
        Assertions.assertEquals(end.minusMonths(3L), rewardTransactionResultAllFilters.getTrxDate());
        Assertions.assertEquals(userId,rewardTransactionResultAllFilters.getUserId());
        Assertions.assertEquals(hpan, rewardTransactionResultAllFilters.getHpan());
        Assertions.assertEquals(acquuirerId, rewardTransactionResultAllFilters.getAcquirerId());

        Assertions.assertNull(resultWithoutMandatoryField);

        Assertions.assertNotNull(resultNotUserIdFilter);
        Assertions.assertEquals(2, resultNotUserIdFilter.count().block());
        resultNotUserIdFilter.toIterable().forEach(rt -> {
            Assertions.assertEquals(hpan, rt.getHpan());
            Assertions.assertEquals(acquuirerId,rt.getAcquirerId());
            Assertions.assertNotNull(rt.getUserId());
            Assertions.assertFalse(rt.getTrxDate().isBefore(start));
            Assertions.assertFalse(rt.getTrxDate().isAfter(end));
        });

    }
}