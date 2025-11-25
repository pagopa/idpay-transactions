package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

public interface PointOfSaleTransactionService {

  Mono<Page<RewardTransaction>> getPointOfSaleTransactions(String merchantId, String initiativeId, String pointOfSaleId, String productGtin, String fiscalCode, String status, Pageable pageable);

  Mono<DownloadInvoiceResponseDTO> downloadTransactionInvoice(String merchantId, String pointOfSaleId, String transactionId);

  Mono<Void> updateInvoiceTransaction(String transactionId, String merchantId, String pointOfSaleId, MultipartFile file, String docNumber);
}
