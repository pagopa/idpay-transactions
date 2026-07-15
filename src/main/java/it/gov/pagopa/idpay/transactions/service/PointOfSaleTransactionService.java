package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicy;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

import java.util.List;

public interface PointOfSaleTransactionService {


    Mono<Void> updateInvoiceTransaction(String transactionId, String merchantId, FilePart file, String docNumber, InvoiceLifecyclePolicy policy);

    Mono<List<FranchisePointOfSaleDTO>> getDistinctFranchiseAndPosByRewardBatchId(String rewardBatchId, String merchantId);

    Mono<Void> reversalTransaction(String transactionId, String merchantId, FilePart file, String docNumber, InvoiceLifecyclePolicy policy);
}