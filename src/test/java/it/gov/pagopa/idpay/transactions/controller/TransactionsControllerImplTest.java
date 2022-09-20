package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.exception.Severity;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.ErrorNotifierService;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionServiceImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@WebFluxTest(controllers = {TransactionsController.class})
@Import(RewardTransactionServiceImpl.class)
class TransactionsControllerImplTest {
    @MockBean
    RewardTransactionRepository rewardTransactionRepository;

    @MockBean
    RewardTransactionMapper rewardTransactionMapper;

    @MockBean
    ErrorNotifierService errorNotifierService;

    @Autowired
    protected WebTestClient webClient;

    @Test
    void findAllOk() {
        LocalDateTime now = LocalDateTime.of(2022, 9, 20, 13, 15,45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .trxDate(now)
                .amount(new BigDecimal("30.00")).build();

        Mockito.when(rewardTransactionRepository.findByFilters(Mockito.eq(rt.getIdTrxIssuer()),Mockito.eq(rt.getUserId()), Mockito.eq(rt.getAmount()), Mockito.any(), Mockito.any()))
                .thenReturn(Flux.just(rt));

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", rt.getIdTrxIssuer())
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amount", rt.getAmount())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findByFilters(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    void findAllBadRequest(){
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .trxDate(now)
                .amount(new BigDecimal("30.00")).build();

        ErrorDTO expectedErrorDTO = new ErrorDTO(Severity.ERROR,"Error", "The mandatory filters are: trxDateStart, trxDateEnd, and one of the following options: 1) idTrxIssuer 2) userId and amount");

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amount", rt.getAmount()).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amount", rt.getAmount())
                        .queryParam("trxDateStart", startDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amount", rt.getAmount())
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        Mockito.verify(rewardTransactionRepository, Mockito.never()).findByFilters(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    void findAllBadRequestWithRangeDate(){
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .trxDate(now)
                .amount(new BigDecimal("30.00")).build();

        ErrorDTO expectedErrorDTO = new ErrorDTO(Severity.ERROR,"Error", "Missing filters. Add one of the following options: 1) idTrxIssuer 2) userId and amount");

        Mockito.when(rewardTransactionRepository.findByFilters(null,rt.getUserId(), null,startDate, endDate))
                .thenThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,expectedErrorDTO.getTitle(), expectedErrorDTO.getMessage()));

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);
        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findByFilters(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }
}