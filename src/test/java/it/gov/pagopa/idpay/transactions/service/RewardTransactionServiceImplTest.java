package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

class RewardTransactionServiceImplTest {
    @Test
    void findByIdTrxIssuer() {
        // Given
        RewardTransactionRepository rewardTransactionRepository = Mockito.mock(RewardTransactionRepository.class);
        RewardTransactionMapper rewardTransactionMapper = Mockito.mock(RewardTransactionMapper.class);
        ErrorNotifierService errorNotifierService = Mockito.mock(ErrorNotifierService.class);
        ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);

        RewardTransactionService rewardTransactionService= new RewardTransactionServiceImpl(rewardTransactionRepository, rewardTransactionMapper,errorNotifierService, objectMapper);

        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amount(new BigDecimal("30.00"))
                .trxDate(LocalDateTime.of(2022, 9, 19, 15,43,39))
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.findByIdTrxIssuerAndOtherFilters(rt.getIdTrxIssuer(), null, null, null, null)).thenReturn(Flux.just(rt));

        // When
        Flux<RewardTransaction> result = rewardTransactionService.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null);
        Assertions.assertNotNull(result);
        RewardTransaction resultRT = result.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(resultRT);
        Assertions.assertEquals(rt, resultRT);
    }

    @Test
    void findByUserIdAndRangeDateAndAmount() {
        // Given
        RewardTransactionRepository rewardTransactionRepository = Mockito.mock(RewardTransactionRepository.class);
        RewardTransactionMapper rewardTransactionMapper = Mockito.mock(RewardTransactionMapper.class);
        ErrorNotifierService errorNotifierService = Mockito.mock(ErrorNotifierService.class);
        ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);

        RewardTransactionService rewardTransactionService= new RewardTransactionServiceImpl(rewardTransactionRepository, rewardTransactionMapper,errorNotifierService, objectMapper);

        LocalDateTime date = LocalDateTime.of(2022, 9, 19, 15,43,39);
        LocalDateTime startDate = date.minusMonths(9L);
        LocalDateTime endDate = date.plusMonths(6L);


        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amount(new BigDecimal("30.00"))
                .trxDate(date)
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.findByIdTrxIssuerAndOtherFilters(null, "USERID", startDate, endDate, null)).thenReturn(Flux.just(rt));

        // When
        Flux<RewardTransaction> result = rewardTransactionService.findByIdTrxIssuer(null, "USERID", startDate, endDate, null);
        Assertions.assertNotNull(result);
        RewardTransaction resultRT = result.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(resultRT);
        Assertions.assertEquals(rt, resultRT);
    }

}
