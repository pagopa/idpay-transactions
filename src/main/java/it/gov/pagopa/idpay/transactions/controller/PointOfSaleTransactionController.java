package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RequestMapping("/idpay")
public interface PointOfSaleTransactionController {


    @PutMapping(path = "/transactions/{transactionId}/invoice/update", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(code = HttpStatus.NO_CONTENT)
    Mono<Void> updateInvoiceFile(
            @PathVariable("transactionId") String transactionId,
            @RequestHeader("x-merchant-id") String merchantId,
            @RequestPart("file") FilePart file,
            @RequestPart(value = "docNumber", required = false) String docNumber,
            @RequestHeader(name = "Authorization", required = true) String authorization
    );


    @GetMapping("/point-of-sales/{rewardBatchId}")
    Mono<List<FranchisePointOfSaleDTO>> getFranchisePointOfSale(
            @PathVariable("rewardBatchId") String rewardBatchId,
            @RequestHeader("x-merchant-id") String merchantId
    );


    @PostMapping("/transactions/{transactionId}/reversal-invoiced")
    @ResponseStatus(code = HttpStatus.NO_CONTENT)
    Mono<Void> reversalTransactionInvoiced(
            @PathVariable("transactionId") String transactionId,
            @RequestHeader("x-merchant-id") String merchantId,
            @RequestHeader(name = "Authorization", required = true) String authorization,
            @RequestPart("file") FilePart file,
            @RequestPart(value = "docNumber", required = false) String docNumber
    );
}