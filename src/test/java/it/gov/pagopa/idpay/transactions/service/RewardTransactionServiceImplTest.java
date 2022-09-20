package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.exception.NotEnoughFiltersException;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

class RewardTransactionServiceImplTest {

    @Test
    void findAll() {
        // Given
        RewardTransactionRepository rewardTransactionRepository = Mockito.mock(RewardTransactionRepository.class);
        RewardTransactionMapper rewardTransactionMapper = Mockito.mock(RewardTransactionMapper.class);
        ErrorNotifierService errorNotifierService = Mockito.mock(ErrorNotifierService.class);
        ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);

        RewardTransactionService rewardTransactionService= new RewardTransactionServiceImpl(rewardTransactionRepository, rewardTransactionMapper,errorNotifierService, objectMapper);

        LocalDateTime date = LocalDateTime.of(2022, 9, 19, 15,43,39);
//        LocalDateTime end = LocalDateTime.now();
        String userId = "USERID";
        String hpan = "HPAN";
        String acquuirerId = " ACQUIRERID";

        RewardTransaction transaction1 = RewardTransaction.builder()
                .userId(userId)
                .hpan(hpan)
                .amount(new BigDecimal("30.00"))
                .acquirerId(acquuirerId).build();
        RewardTransaction transaction2 = RewardTransaction.builder()
                .trxDate(date)
                .userId("ANOTHER_USERID")
                .hpan(hpan)
                .amount(new BigDecimal("100.00"))
                .acquirerId(acquuirerId).build();
        RewardTransaction transaction3 = RewardTransaction.builder()
                .trxDate(date)
                .userId(userId)
                .hpan("ANOTHER_HPAN")
                .idTrxIssuer("IDTRXISSUER")
                .acquirerId(acquuirerId).build();

        Mockito.when(rewardTransactionRepository.findByFilters(transaction1.getIdTrxIssuer(), transaction1.getUserId(), transaction1.getTrxDate(),transaction1.getAmount())).thenThrow(NotEnoughFiltersException.class);
        Mockito.when(rewardTransactionRepository.findByFilters(transaction2.getIdTrxIssuer(), transaction2.getUserId(), transaction2.getTrxDate(),transaction2.getAmount())).thenReturn(Flux.just(transaction2));
        Mockito.when(rewardTransactionRepository.findByFilters(transaction3.getIdTrxIssuer(), transaction3.getUserId(), transaction3.getTrxDate(),transaction3.getAmount())).thenReturn(Flux.just(transaction3));

        // When
        Flux<RewardTransaction> resultNotEnoughFilter = rewardTransactionService.findTrxsFilters(transaction1.getIdTrxIssuer(), transaction1.getUserId(), null, transaction1.getAmount().toString());
        Flux<RewardTransaction> resultWithFilter = rewardTransactionService.findTrxsFilters(transaction2.getIdTrxIssuer(), transaction2.getUserId(), transaction2.getTrxDate().format(DateTimeFormatter.ISO_DATE_TIME), transaction2.getAmount().toString());
        Flux<RewardTransaction> resultWithIdTrxIssuer = rewardTransactionService.findTrxsFilters(transaction3.getIdTrxIssuer(), transaction3.getUserId(), transaction3.getTrxDate().format(DateTimeFormatter.ISO_DATE_TIME), null);

        // Then
        Assertions.assertNotNull(resultWithFilter);
        Assertions.assertEquals(1, resultWithFilter.count().block());

        List<RewardTransaction> rtListResultWithFilter = resultWithFilter.collectList().block();
        Assertions.assertNotNull(rtListResultWithFilter);
        RewardTransaction rtResultWithFilter = rtListResultWithFilter.stream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultWithFilter);
        Assertions.assertEquals(transaction3, rtResultWithFilter);

        Assertions.assertNotNull(resultWithIdTrxIssuer);
        Assertions.assertEquals(1, resultWithIdTrxIssuer.count().block());

        List<RewardTransaction> rtListResultWithIdTrxIssuer = resultWithIdTrxIssuer.collectList().block();
        Assertions.assertNotNull(rtListResultWithIdTrxIssuer);
        RewardTransaction rtResultWithIdTrxIssuer = rtListResultWithIdTrxIssuer.stream().findFirst().orElse(null);
        Assertions.assertNotNull(rtResultWithIdTrxIssuer);
        Assertions.assertEquals(transaction3, rtResultWithIdTrxIssuer);

        //TODO verify
//        Mockito.verify(rewardTransactionRepository, Mockito.times(3)).findByIdTrxIssuer(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }
}
