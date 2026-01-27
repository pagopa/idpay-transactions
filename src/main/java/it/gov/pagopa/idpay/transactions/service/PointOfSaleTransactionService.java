package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

import java.util.List;

public interface PointOfSaleTransactionService {

  Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId,
                                                           String initiativeId,
                                                           String pointOfSaleId,
                                                           String productGtin,
                                                           String fiscalCode,
                                                           String status,
                                                           String trxCode,
                                                           Pageable pageable);

  Mono<DownloadInvoiceResponseDTO> downloadTransactionInvoice(String merchantId, String pointOfSaleId, String transactionId);

  Mono<Void> updateInvoiceTransaction(String transactionId, String merchantId, String pointOfSaleId, FilePart file, String docNumber);


  Mono<List<FranchisePointOfSaleDTO>> getDistinctFranchiseAndPosByRewardBatchId(String rewardBatchId);
  Mono<Void> reversalTransaction(String transactionId, String merchantId, String pointOfSaleId, FilePart file, String docNumber);
}
