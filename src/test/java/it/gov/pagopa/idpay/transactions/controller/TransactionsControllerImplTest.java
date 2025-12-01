package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import reactor.core.publisher.Mono;

@WebFluxTest(controllers = {TransactionsController.class})
class TransactionsControllerImplTest {
    @MockitoBean
    RewardTransactionService rewardTransactionService;

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
                .amountCents(3000L).build();

        //idTrxIssuer present in request
        Mockito.when(rewardTransactionService.findByIdTrxIssuer(Mockito.eq(rt.getIdTrxIssuer()),Mockito.eq(rt.getUserId()), Mockito.any(), Mockito.any(), Mockito.eq(rt.getAmountCents()), Mockito.any()))
                .thenReturn(Flux.just(rt));

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", rt.getIdTrxIssuer())
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

        //userId e range of date present in the request
        Mockito.when(rewardTransactionService.findByRange(Mockito.eq(rt.getUserId()), Mockito.any(), Mockito.any(), Mockito.eq(rt.getAmountCents()), Mockito.any()))
                .thenReturn(Flux.just(rt));

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

        Mockito.verify(rewardTransactionService, Mockito.times(1)).findByIdTrxIssuer(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
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
                .amountCents(3000L).build();

        ErrorDTO expectedErrorDTO = new ErrorDTO(ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ExceptionConstants.ExceptionMessage.TRANSACTIONS_MISSING_MANDATORY_FILTERS);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents()).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("trxDateStart", startDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("amountCents", rt.getAmountCents()).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        Mockito.verify(rewardTransactionService, Mockito.never()).findByRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(rewardTransactionService, Mockito.never()).findByIdTrxIssuer(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    void pageable(){
        LocalDateTime now = LocalDateTime.of(2022, 9, 20, 13, 15,45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);
        String userId = "USERID";
        String idTrxIssuer = "IDTRXISSUER";
        Long amountCents = 3000L;

        RewardTransaction rt = RewardTransaction.builder()
                .id("ID1")
                .idTrxIssuer(idTrxIssuer)
                .userId(userId)
                .trxDate(now)
                .amountCents(amountCents).build();

        //idTrxIssuer present in request
        Mockito.when(rewardTransactionService.findByIdTrxIssuer(Mockito.any(),Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(amountCents), Mockito.any()))
                .thenReturn(Flux.just(rt));

        Pageable expectedPageable = PageRequest.of(2, 3, Sort.unsorted());

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", idTrxIssuer)
                        .queryParam("userId", userId)
                        .queryParam("amountCents", amountCents)
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .queryParam("page", expectedPageable.getPageNumber())
                        .queryParam("size", expectedPageable.getPageSize())
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);
        Mockito.verify(rewardTransactionService, Mockito.times(1)).findByIdTrxIssuer(Mockito.eq(rt.getIdTrxIssuer()), Mockito.any(), Mockito.any(),Mockito.any(),Mockito.any(),Mockito.eq(expectedPageable));

        Pageable expectedPageable2 = PageRequest.of(0, 3, Sort.Direction.DESC, "_id");
        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "idTrxIssuer2")
                        .queryParam("userId", userId)
                        .queryParam("amountCents", amountCents)
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .queryParam("page", expectedPageable2.getPageNumber())
                        .queryParam("size", expectedPageable2.getPageSize())
                        .queryParam("sort", "_id,desc")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);
        Mockito.verify(rewardTransactionService, Mockito.times(1)).findByIdTrxIssuer(Mockito.eq("idTrxIssuer2"), Mockito.any(), Mockito.any(),Mockito.any(),Mockito.any(),Mockito.eq(expectedPageable2));

    }

    @Test
    void cleanupInvoicedTransactions_defaultChunkSize() {
        Mockito.when(rewardTransactionService.assignInvoicedTransactionsToBatches(Mockito.anyInt(), Mockito.anyBoolean(), Mockito.isNull()))
            .thenReturn(Mono.empty());

        webClient.post()
            .uri("/idpay/transactions/cleanup")
            .exchange()
            .expectStatus().isOk()
            .expectBody().isEmpty();

        Mockito.verify(rewardTransactionService, Mockito.times(1))
            .assignInvoicedTransactionsToBatches(
                Mockito.eq(200),
                Mockito.eq(false),
                Mockito.isNull());
    }

    @Test
    void cleanupInvoicedTransactions_customChunkSize() {
        Mockito.when(rewardTransactionService.assignInvoicedTransactionsToBatches(Mockito.anyInt(), Mockito.anyBoolean(), Mockito.isNull()))
            .thenReturn(Mono.empty());

        int customChunkSize = 500;

        webClient.post()
            .uri(uriBuilder -> uriBuilder.path("/idpay/transactions/cleanup")
                .queryParam("chunkSize", customChunkSize)
                .build())
            .exchange()
            .expectStatus().isOk()
            .expectBody().isEmpty();

        Mockito.verify(rewardTransactionService, Mockito.times(1))
            .assignInvoicedTransactionsToBatches(
                Mockito.eq(customChunkSize),
                Mockito.eq(false),
                Mockito.isNull()
            );
    }
}