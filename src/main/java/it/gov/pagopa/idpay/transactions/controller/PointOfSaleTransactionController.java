package it.gov.pagopa.idpay.transactions.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay")
public interface PointOfSaleTransactionController {

  @GetMapping("/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
  Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(
      @RequestHeader("x-merchant-id") String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PathVariable("pointOfSaleId") String pointOfSaleId,
      @RequestParam(required = false) String productGtin,
      @RequestParam(required = false) String fiscalCode,
      @RequestParam(required = false) String status,
      @PageableDefault(sort = "elaborationDateTime", direction = Sort.Direction.DESC) Pageable pageable);

  /**
   * Provides method to download an invoice file of a rewarded transaction
   * @param merchantId obtained through a header parameter, used to check if the transaction required is referred to the same merchant
   * @param pointOfSaleId obtained through a header parameter, used to check if the transaction required is referred to the same point of sale
   * @param transactionId
   * @return Mono containing an instance of DownloadInvoiceResponseDTO, containing the invoice url
   */
  @Operation(summary = "downloadInvoiceFile",
          description = "Return invoice file download URL")
  @ApiResponses(value = {
          @ApiResponse(responseCode = "200", description = "Return file signed url",
                  content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                          schema = @Schema(implementation = DownloadInvoiceResponseDTO.class)
                  )),
          @ApiResponse(responseCode = "400", description = "Bad Request",
                  content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                          schema = @Schema(implementation = ErrorDTO.class))),
          @ApiResponse(responseCode = "401",
                  description = "Unauthorized", content = @Content(schema = @Schema())),
          @ApiResponse(responseCode = "403",
                  description = "Forbidden", content = @Content(schema = @Schema())),
          @ApiResponse(responseCode = "404", description = "Invoice not found",
                  content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                          schema = @Schema(implementation = ErrorDTO.class))),
          @ApiResponse(responseCode = "429",
                  description = "Too many requests", content = @Content(schema = @Schema())),
          @ApiResponse(responseCode = "500",
                  description = "Service error", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                  schema = @Schema(implementation = ErrorDTO.class))),
          @ApiResponse(responseCode = "503",
                  description = "Service unavailable",
                  content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                          schema = @Schema(implementation = ErrorDTO.class)))
  })
  @GetMapping("/{pointOfSaleId}/transactions/{transactionId}/download")
  Mono<DownloadInvoiceResponseDTO> downloadInvoiceFile(
      @RequestHeader("x-merchant-id") String merchantId,
      @PathVariable("pointOfSaleId") String pointOfSaleId,
      @PathVariable("transactionId") String transactionId);
}
