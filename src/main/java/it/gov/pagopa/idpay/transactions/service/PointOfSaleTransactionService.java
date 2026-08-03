package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface PointOfSaleTransactionService {

    Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId,
                                                             String initiativeId,
                                                             String pointOfSaleId,
                                                             String productGtin,
                                                             TrxFiltersDTO filters,
                                                             Pageable pageable);

    Mono<DownloadInvoiceResponseDTO> downloadTransactionInvoice(String merchantId, String pointOfSaleId, String transactionId);

    Mono<List<FranchisePointOfSaleDTO>> getDistinctFranchiseAndPosByRewardBatchId(String rewardBatchId, String merchantId);
}