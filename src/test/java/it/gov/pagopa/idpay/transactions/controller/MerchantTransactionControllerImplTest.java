package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MerchantTransactionControllerImplTest {

    @Mock
    private MerchantTransactionService merchantTransactionService;

    @InjectMocks
    private MerchantTransactionControllerImpl merchantTransactionController;

    @Test
    void getMerchantTransactions_shouldDelegateToServiceAndReturnResult() {
        String merchantId = "MERCHANT_ID";
        String initiativeId = "INITIATIVE_ID";
        String fiscalCode = "FISCAL_CODE";
        String status = "STATUS";
        String rewardBatchId = "REWARD_BATCH_ID";
        String rewardBatchTrxStatus = "REWARD_BATCH_TRX_STATUS";
        Pageable pageable = PageRequest.of(0, 20);

        MerchantTransactionsListDTO expectedDto = new MerchantTransactionsListDTO();

        when(merchantTransactionService.getMerchantTransactions(
                merchantId,
                initiativeId,
                fiscalCode,
                status,
                rewardBatchId,
                rewardBatchTrxStatus,
                null,
                pageable))
                .thenReturn(Mono.just(expectedDto));

        MerchantTransactionsListDTO result = merchantTransactionController
                .getMerchantTransactions(
                        merchantId,
                        initiativeId,
                        fiscalCode,
                        status,
                        rewardBatchId,
                        rewardBatchTrxStatus,
                        null,
                        pageable)
                .block();

        assertSame(expectedDto, result);

        verify(merchantTransactionService).getMerchantTransactions(
                merchantId,
                initiativeId,
                fiscalCode,
                status,
                rewardBatchId,
                rewardBatchTrxStatus,
                null,
                pageable
        );
    }
}
