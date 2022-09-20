package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.exception.NotEnoughFiltersException;
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
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;

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
        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .amount(new BigDecimal("30.00")).build();

        Mockito.when(rewardTransactionRepository.findByFilters(rt.getIdTrxIssuer(),rt.getUserId(),null,rt.getAmount()))
                .thenReturn(Flux.just(rt));

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", rt.getIdTrxIssuer())
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amount", "30.00").build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findByFilters(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    void findAllBadRequest(){
        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .amount(new BigDecimal("30.00")).build();

        ErrorDTO expectedErrorDTO = new ErrorDTO(Severity.ERROR, "Error", null);

        Mockito.when(rewardTransactionRepository.findByFilters(null,rt.getUserId(),null,rt.getAmount()))
                .thenThrow(NotEnoughFiltersException.class);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amount", "30.00").build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findByFilters(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }
}