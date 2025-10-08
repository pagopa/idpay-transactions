package it.gov.pagopa.idpay.transactions.service;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.ClientException;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceFile;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.storage.InvoiceStorageClient;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

  @Mock
  private UserRestClient userRestClient;

  @Mock
  private RewardTransactionRepository rewardTransactionRepository;

  @Mock
  private InvoiceStorageClient invoiceStorageClient;

  private PointOfSaleTransactionService pointOfSaleTransactionService;

  private final Pageable pageable = PageRequest.of(0, 10);

  private static final String MERCHANT_ID = "MERCHANTID1";
  private static final String INITIATIVE_ID = "INITIATIVEID1";
  private static final String POINT_OF_SALE_ID = "POINTOFSALEID1";
  private static final String FISCAL_CODE = "FISCALCODE1";
  private static final String USER_ID = "USERID1";
  private static final String STATUS = "REWARDED";
  private static final String TRX_ID = "TRX_ID";


  @BeforeEach
  void setUp() {
    Mockito.reset(rewardTransactionRepository, invoiceStorageClient);
    pointOfSaleTransactionService = new PointOfSaleTransactionServiceImpl(userRestClient,
        rewardTransactionRepository, invoiceStorageClient);
  }

  @Test
  void getPointOfSaleTransactionsWithFiscalCode() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(1);

    when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE)).thenReturn(
        Mono.just(new FiscalCodeInfoPDV(USER_ID)));

    when(rewardTransactionRepository.findByFilterTrx(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID,
        USER_ID, "", STATUS, pageable))
        .thenReturn(Flux.just(trx));

    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID,
        "", STATUS))
        .thenReturn(Mono.just(1L));

    Mono<Page<RewardTransaction>> result = pointOfSaleTransactionService
        .getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, "", FISCAL_CODE,
            STATUS, pageable);

    StepVerifier.create(result)
        .assertNext(page -> {
          assertEquals(1, page.getTotalElements());
          assertEquals(1, page.getContent().size());
          assertEquals(trx.getIdTrxAcquirer(), page.getContent().get(0).getIdTrxAcquirer());
        })
        .verifyComplete();
  }

  @Test
  void getPointOfSaleTransactionsWithoutFiscalCode() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(2);

    when(rewardTransactionRepository.findByFilterTrx(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID,
        null, null, STATUS, pageable))
        .thenReturn(Flux.just(trx));

    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null,
        null, STATUS))
        .thenReturn(Mono.just(1L));

    Mono<Page<RewardTransaction>> result = pointOfSaleTransactionService
        .getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, null,
            STATUS, pageable);

    StepVerifier.create(result)
        .assertNext(page -> {
          assertEquals(1, page.getTotalElements());
          assertEquals(1, page.getContent().size());
          assertEquals(trx.getIdTrxAcquirer(), page.getContent().get(0).getIdTrxAcquirer());
        })
        .verifyComplete();
  }

  @Test
  void shouldReturnErrorIfValidRecoveryWithMissingFileName() {
    RewardTransaction rewardTransaction = RewardTransaction.builder()
        .invoiceFile(InvoiceFile.builder().build())
        .build();
    doReturn(Mono.just(rewardTransaction)).when(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    lenient().doReturn("tokenUrl").when(invoiceStorageClient).getFileSignedUrl("filename");
    Mono<DownloadInvoiceResponseDTO> downloadInvoiceResponseDTOMono =
        assertDoesNotThrow(() ->
            pointOfSaleTransactionService.downloadTransactionInvoice(
                MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID));
    assertNotNull(downloadInvoiceResponseDTOMono);
    StepVerifier.create(downloadInvoiceResponseDTOMono).assertNext(
        downloadInvoiceResponseDTO -> {
          assertNotNull(downloadInvoiceResponseDTO.getInvoiceUrl());
          assertEquals("tokenUrl", downloadInvoiceResponseDTO.getInvoiceUrl());
        });
    verify(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    verifyNoInteractions(invoiceStorageClient);
  }

  @Test
  void shouldThrowMissingInvoiceWhenInvoiceFileIsNullOrFilenameIsNull() {
    RewardTransaction trxWithoutInvoiceFile = RewardTransaction.builder().invoiceFile(null).build();
    doReturn(Mono.just(trxWithoutInvoiceFile)).when(rewardTransactionRepository)
        .findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);

    Mono<DownloadInvoiceResponseDTO> result1 = pointOfSaleTransactionService.downloadTransactionInvoice(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);

    StepVerifier.create(result1)
        .expectErrorMatches(throwable -> {
          assert throwable instanceof ClientExceptionNoBody;
          ClientExceptionNoBody ex = (ClientExceptionNoBody) throwable;
          return ex.getHttpStatus() == HttpStatus.BAD_REQUEST
              && ex.getMessage().equals(TRANSACTION_MISSING_INVOICE);
        })
        .verify();

    RewardTransaction trxWithNullFilename = RewardTransaction.builder()
        .invoiceFile(InvoiceFile.builder().filename(null).build())
        .build();
    doReturn(Mono.just(trxWithNullFilename)).when(rewardTransactionRepository)
        .findTransaction(MERCHANT_ID, POINT_OF_SALE_ID, "TRX_ID_2");

    Mono<DownloadInvoiceResponseDTO> result2 = pointOfSaleTransactionService.downloadTransactionInvoice(
        MERCHANT_ID, POINT_OF_SALE_ID, "TRX_ID_2");

    StepVerifier.create(result2)
        .expectErrorMatches(throwable -> {
          assert throwable instanceof ClientExceptionNoBody;
          ClientExceptionNoBody ex = (ClientExceptionNoBody) throwable;
          return ex.getHttpStatus() == HttpStatus.BAD_REQUEST
              && ex.getMessage().equals(TRANSACTION_MISSING_INVOICE);
        })
        .verify();

    verify(rewardTransactionRepository, times(2)).findTransaction(any(), any(), any());
    verifyNoInteractions(invoiceStorageClient);
  }

  @Test
  void shouldReturnDownloadUrlIfValidRecovery() {
    RewardTransaction rewardTransaction = RewardTransaction.builder()
        .invoiceFile(InvoiceFile.builder().filename("filename").build())
        .build();
    doReturn(Mono.just(rewardTransaction)).when(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    lenient().doReturn("tokenUrl").when(invoiceStorageClient).getFileSignedUrl(
        "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/filename");
    Mono<DownloadInvoiceResponseDTO> downloadInvoiceResponseDTOMono =
        assertDoesNotThrow(() ->
            pointOfSaleTransactionService.downloadTransactionInvoice(
                MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID));
    assertNotNull(downloadInvoiceResponseDTOMono);
    StepVerifier.create(downloadInvoiceResponseDTOMono).assertNext(
        downloadInvoiceResponseDTO -> {
          assertNotNull(downloadInvoiceResponseDTO.getInvoiceUrl());
          assertEquals("tokenUrl", downloadInvoiceResponseDTO.getInvoiceUrl());
        }).verifyComplete();
    verify(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    verify(invoiceStorageClient).getFileSignedUrl(
        "invoices/merchant/MERCHANTID1/pos/POINTOFSALEID1/transaction/TRX_ID/filename");
  }

  @Test
  void shouldThrowMissingErrorOnInvalidRecoveryForMerchant() {
    doReturn(Mono.empty()).when(rewardTransactionRepository).findTransaction(
        "AAAA", POINT_OF_SALE_ID, TRX_ID);
    Mono<DownloadInvoiceResponseDTO> responseDTOMono = assertDoesNotThrow(() ->
        pointOfSaleTransactionService.downloadTransactionInvoice(
            "AAAA", POINT_OF_SALE_ID, TRX_ID));
    StepVerifier.create(responseDTOMono)
        .expectErrorMatches(throwable -> {
          assert throwable instanceof ClientException;
          Boolean isRightCode = ((ClientException) throwable).getHttpStatus()
              .equals(HttpStatus.BAD_REQUEST);
          Boolean isRightMessage = throwable.getMessage().equals(TRANSACTION_MISSING_INVOICE);
          return isRightMessage && isRightCode;
        })
        .verify();
    verify(rewardTransactionRepository).findTransaction(
        "AAAA", POINT_OF_SALE_ID, TRX_ID);
    verifyNoInteractions(invoiceStorageClient);
  }

  @Test
  void shouldThrowMissingErrorOnInvalidRecoveryForInitiative() {
    doReturn(Mono.empty()).when(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    Mono<DownloadInvoiceResponseDTO> responseDTOMono = assertDoesNotThrow(() ->
        pointOfSaleTransactionService.downloadTransactionInvoice(
            MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID));
    StepVerifier.create(responseDTOMono)
        .expectErrorMatches(throwable -> {
          assert throwable instanceof ClientException;
          Boolean isRightCode = ((ClientException) throwable).getHttpStatus()
              .equals(HttpStatus.BAD_REQUEST);
          Boolean isRightMessage = throwable.getMessage().equals(TRANSACTION_MISSING_INVOICE);
          return isRightMessage && isRightCode;
        })
        .verify();
    verify(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    verifyNoInteractions(invoiceStorageClient);
  }

  @Test
  void shouldThrowMissingErrorOnInvalidRecoveryForPointOfSale() {
    doReturn(Mono.empty()).when(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, "AAAAA", TRX_ID);
    Mono<DownloadInvoiceResponseDTO> responseDTOMono = assertDoesNotThrow(() ->
        pointOfSaleTransactionService.downloadTransactionInvoice(
            MERCHANT_ID, "AAAAA", TRX_ID));
    StepVerifier.create(responseDTOMono)
        .expectErrorMatches(throwable -> {
          assert throwable instanceof ClientException;
          Boolean isRightCode = ((ClientException) throwable).getHttpStatus()
              .equals(HttpStatus.BAD_REQUEST);
          Boolean isRightMessage = throwable.getMessage().equals(TRANSACTION_MISSING_INVOICE);
          return isRightMessage && isRightCode;
        })
        .verify();
    verify(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, "AAAAA", TRX_ID);
    verifyNoInteractions(invoiceStorageClient);
  }

  @Test
  void shouldThrowMissingErrorOnInvalidRecoveryForTrxId() {
    doReturn(Mono.empty()).when(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");
    Mono<DownloadInvoiceResponseDTO> responseDTOMono = assertDoesNotThrow(() ->
        pointOfSaleTransactionService.downloadTransactionInvoice(
            MERCHANT_ID, POINT_OF_SALE_ID, "AAAA"));

    StepVerifier.create(responseDTOMono)
        .expectErrorMatches(throwable -> {
          assert throwable instanceof ClientException;
          Boolean isRightCode = ((ClientException) throwable).getHttpStatus()
              .equals(HttpStatus.BAD_REQUEST);
          Boolean isRightMessage = throwable.getMessage().equals(TRANSACTION_MISSING_INVOICE);
          return isRightMessage && isRightCode;
        })
        .verify();

    verify(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");
    verifyNoInteractions(invoiceStorageClient);
  }

  @Test
  void shouldThrowMissingErrorOnFileUrlRecoveryError() {
    RewardTransaction rewardTransaction = RewardTransaction.builder()
        .invoiceFile(InvoiceFile.builder().filename("filename").build())
        .build();
    doReturn(Mono.just(rewardTransaction)).when(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");
    doThrow(new ClientException(HttpStatus.INTERNAL_SERVER_ERROR, ERROR_ON_GET_FILE_URL_REQUEST))
        .when(invoiceStorageClient).getFileSignedUrl(anyString());

    Mono<DownloadInvoiceResponseDTO> responseDTOMono =
        pointOfSaleTransactionService.downloadTransactionInvoice(
            MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");

    StepVerifier.create(responseDTOMono)
        .expectErrorSatisfies(throwable -> {
          assertInstanceOf(ClientException.class, throwable);
          ClientException ex = (ClientException) throwable;
          assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, ex.getHttpStatus());
          assertEquals(ERROR_ON_GET_FILE_URL_REQUEST, ex.getMessage());
        })
        .verify();

    verify(rewardTransactionRepository).findTransaction(
        MERCHANT_ID, POINT_OF_SALE_ID, "AAAA");
    verify(invoiceStorageClient).getFileSignedUrl(anyString());
  }


}
