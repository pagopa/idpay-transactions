package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@WebFluxTest(controllers = {MerchantTransactionController.class})
class MerchantTransactionControllerImplTest {
    @MockBean
    MerchantTransactionService merchantTransactionService;

    @Autowired
    protected WebTestClient webClient;

    @Test
    void findMerchantTransactionsOk() {

        MerchantTransactionsListDTO merchantTransactionsListDTO = MerchantTransactionsListDTO.builder()
                .pageNo(0)
                .pageSize(10)
                .totalElements(1)
                .totalPages(1).build();

        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        //no filter
        Mockito.when(merchantTransactionService.getMerchantTransactions("test", "INITIATIVE_ID", null, null, paging))
                .thenReturn(Mono.just(merchantTransactionsListDTO));

        webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed")
                        .build("INITIATIVE_ID"))
                .header("x-merchant-id", "test")
                .exchange()
                .expectStatus().isOk()
                .expectBody(MerchantTransactionsListDTO.class).isEqualTo(merchantTransactionsListDTO);

        Mockito.verify(merchantTransactionService, Mockito.times(1)).getMerchantTransactions("test", "INITIATIVE_ID", null, null, paging);
    }

}
