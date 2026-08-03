package it.gov.pagopa.idpay.transactions.service;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoiceTransactionLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSearchPort;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class PointOfSaleTransactionServiceImpl implements PointOfSaleTransactionService {

    private final UserRestClient userRestClient;
    private final RewardTransactionSearchPort rewardTransactionSearchPort;
    private final InvoiceTransactionLookupPort invoiceTransactionLookupPort;
    private final RewardBatchTransactionReadPort rewardBatchTransactionReadPort;
    private final InvoiceStorageClient invoiceStorageClient;

    protected PointOfSaleTransactionServiceImpl(
            UserRestClient userRestClient,
            RewardTransactionSearchPort rewardTransactionSearchPort,
            InvoiceTransactionLookupPort invoiceTransactionLookupPort,
            RewardBatchTransactionReadPort rewardBatchTransactionReadPort,
            InvoiceStorageClient invoiceStorageClient
    ) {
        this.userRestClient = userRestClient;
        this.rewardTransactionSearchPort = rewardTransactionSearchPort;
        this.invoiceTransactionLookupPort = invoiceTransactionLookupPort;
        this.rewardBatchTransactionReadPort = rewardBatchTransactionReadPort;
        this.invoiceStorageClient = invoiceStorageClient;
    }

    @Override
    public Mono<Page<RewardTransaction>> getPointOfSaleTransactions(
            String merchantId,
            String initiativeId,
            String pointOfSaleId,
            String productGtin,
            TrxFiltersDTO filters,
            Pageable pageable
    ) {
        filters.setMerchantId(merchantId);
        filters.setInitiativeId(initiativeId);

        if (StringUtils.isNotBlank(filters.getFiscalCode())) {
            return userRestClient.retrieveFiscalCodeInfo(filters.getFiscalCode())
                    .map(FiscalCodeInfoPDV::getToken)
                    .flatMap(userId -> getTransactions(
                            filters,
                            pointOfSaleId,
                            userId,
                            productGtin,
                            pageable
                    ));
        }
        return getTransactions(filters, pointOfSaleId, null, productGtin, pageable);
    }

    @Override
    public Mono<DownloadInvoiceResponseDTO> downloadTransactionInvoice(
            String merchantId,
            String pointOfSaleId,
            String transactionId
    ) {
        return invoiceTransactionLookupPort.findInvoiceTransaction(merchantId, transactionId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(
                        HttpStatus.BAD_REQUEST,
                        TRANSACTION_MISSING_INVOICE
                )))
                .handle((rewardTransaction, sink) -> {
                    String status = rewardTransaction.getStatus();
                    InvoiceData documentData;
                    String typeFolder;

                    if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status)
                            || SyncTrxStatus.REWARDED.name().equalsIgnoreCase(status)) {
                        documentData = rewardTransaction.getInvoiceData();
                        typeFolder = "invoice";
                    } else if (SyncTrxStatus.REFUNDED.name().equalsIgnoreCase(status)) {
                        documentData = rewardTransaction.getCreditNoteData();
                        typeFolder = "creditNote";
                    } else {
                        sink.error(new ClientExceptionNoBody(
                                HttpStatus.BAD_REQUEST,
                                TRANSACTION_MISSING_INVOICE
                        ));
                        return;
                    }

                    if (documentData == null || documentData.getFilename() == null) {
                        sink.error(new ClientExceptionNoBody(
                                HttpStatus.BAD_REQUEST,
                                TRANSACTION_MISSING_INVOICE
                        ));
                        return;
                    }

                    String blobPath = String.format(
                            "invoices/merchant/%s/pos/%s/transaction/%s/%s/%s",
                            merchantId,
                            pointOfSaleId,
                            transactionId,
                            typeFolder,
                            documentData.getFilename()
                    );
                    sink.next(DownloadInvoiceResponseDTO.builder()
                            .invoiceUrl(invoiceStorageClient.getFileSignedUrl(blobPath))
                            .build());
                });
    }

    @Override
    public Mono<List<FranchisePointOfSaleDTO>> getDistinctFranchiseAndPosByRewardBatchId(
            String rewardBatchId,
            String merchantId
    ) {
        log.info(
                "[POINT_OF_SALE_TRANSACTION_SERVICE] - Get point of sale for reward batch id [{}]",
                Utilities.sanitizeString(rewardBatchId)
        );
        return rewardBatchTransactionReadPort
                .findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId, merchantId)
                .collectList();
    }

    private Mono<Page<RewardTransaction>> getTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String userId,
            String productGtin,
            Pageable pageable
    ) {
        boolean includeToCheckWithConsultable = false;
        return rewardTransactionSearchPort.findPointOfSaleTransactions(
                        filters,
                        pointOfSaleId,
                        userId,
                        productGtin,
                        includeToCheckWithConsultable,
                        pageable
                )
                .collectList()
                .zipWith(rewardTransactionSearchPort.countPointOfSaleTransactions(
                        filters,
                        pointOfSaleId,
                        productGtin,
                        userId,
                        includeToCheckWithConsultable
                ))
                .map(tuple -> new PageImpl<>(tuple.getT1(), pageable, tuple.getT2()));
    }
}
