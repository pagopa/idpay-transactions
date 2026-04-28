package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Exceptions;
import reactor.test.StepVerifier;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.MERCHANT_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_MERCHANT_NOT_FOUND;

@ContextConfiguration(
        classes = {
                MerchantRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                "spring.cloud.azure.keyvault.enabled=false",
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.merchant.base-url"
        }
)

class MerchantRestClientImplTest extends BaseWireMockTest {

  @Autowired
  private MerchantRestClient merchantRestClient;

  @Test
  void getPointOfSaleOk() {
    String merchantId = "MERCHANT_OK_1";
    String pointOfSaleId = "POS_OK_1";

    PointOfSaleDTO result = merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();

    Assertions.assertNotNull(result);
  }

  @ParameterizedTest
  @CsvSource({
          "MERCHANT_NOTFOUND_1,POS_NOTFOUND_1",
          "MERCHANT_OK_1,POS_NOTFOUND_1"
  })
  void getPointOfSale_NotFound(String merchantId, String pointOfSaleId) {
    PointOfSaleDTO result = merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();

    Assertions.assertNull(result);
  }

  @Test
  void getPointOfSaleInternalServerError() {
    String merchantId = "MERCHANT_INTERNALSERVERERROR_1";
    String pointOfSaleId = "POS_INTERNALSERVERERROR_1";

    try {
      merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();
      Assertions.fail("Expected WebClientResponseException.InternalServerError");
    } catch (Throwable e) {
      Assertions.assertInstanceOf(WebClientException.class, e);
      Assertions.assertEquals(WebClientResponseException.InternalServerError.class, e.getClass());
    }
  }

  @Test
  void getPointOfSaleTooManyRequest() {
    String merchantId = "MERCHANT_TOOMANYREQUEST_1";
    String pointOfSaleId = "POS_TOOMANYREQUEST_1";

    try {
      merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();
      Assertions.fail("Expected retry exhausted exception");
    } catch (Throwable e) {
      Assertions.assertTrue(Exceptions.isRetryExhausted(e));
    }
  }

  @Test
  void getPointOfSaleHttpForbidden() {
    String merchantId = "MERCHANT_FORBIDDEN_1";
    String pointOfSaleId = "POS_FORBIDDEN_1";

    try {
      merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();
      Assertions.fail("Expected WebClientResponseException.Forbidden");
    } catch (Throwable e) {
      Assertions.assertInstanceOf(WebClientException.class, e);
      Assertions.assertEquals(WebClientResponseException.Forbidden.class, e.getClass());
    }
  }

  @Test
  void getPointOfSaleUnauthorized() {
    String merchantId = "MERCHANT_UNAUTHORIZED_1";
    String pointOfSaleId = "POS_UNAUTHORIZED_1";

    try {
      merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();
      Assertions.fail("Expected WebClientResponseException.Unauthorized");
    } catch (Throwable e) {
      Assertions.assertInstanceOf(WebClientException.class, e);
      Assertions.assertEquals(WebClientResponseException.Unauthorized.class, e.getClass());
    }
  }

  @Test
  void getPointOfSale_BadRequest_Single() {
    String merchantId = "MERCHANT_BADREQUEST_1";
    String pointOfSaleId = "POS_BADREQUEST_1";

    PointOfSaleDTO result = merchantRestClient.getPointOfSale(merchantId, pointOfSaleId).block();
    Assertions.assertNull(result);
  }

  @Test
  void getMerchantDetailOk() {
    String merchantId = "MERCHANT_OK_1";
    String initiativeId = "INIT_OK_1";

    MerchantDetailDTO result = merchantRestClient.getMerchantDetail(merchantId, initiativeId).block();

    Assertions.assertNotNull(result);
  }


  @ParameterizedTest
  @CsvSource({
          "MERCHANT_NOTFOUND_1,INIT_NOTFOUND_1",
          "MERCHANT_OK_1,INIT_NOTFOUND_1"
  })
  void getMerchantDetail_NotFound(String merchantId, String initiativeId) {

    StepVerifier.create(merchantRestClient.getMerchantDetail(merchantId, initiativeId))
            .expectErrorSatisfies(error -> {
              Assertions.assertInstanceOf(ClientExceptionWithBody.class, error);

              ClientExceptionWithBody ex = (ClientExceptionWithBody) error;

              Assertions.assertEquals(HttpStatus.NOT_FOUND, ex.getHttpStatus());
              Assertions.assertEquals(MERCHANT_NOT_FOUND, ex.getCode());
              Assertions.assertEquals(
                      ERROR_MESSAGE_MERCHANT_NOT_FOUND.formatted(merchantId, initiativeId),
                      ex.getMessage()
              );
            })
            .verify();
  }



  @Test
  void getMerchantDetailInternalServerError() {
    String merchantId = "MERCHANT_INTERNALSERVERERROR_1";
    String initiativeId = "INIT_INTERNALSERVERERROR_1";

    try {
      merchantRestClient.getMerchantDetail(merchantId, initiativeId).block();
      Assertions.fail("Expected WebClientResponseException.InternalServerError");
    } catch (Throwable e) {
      Assertions.assertEquals(WebClientResponseException.InternalServerError.class, e.getClass());
    }
  }

  @Test
  void getMerchantDetailTooManyRequest() {
    String merchantId = "MERCHANT_TOOMANYREQUEST_1";
    String initiativeId = "INIT_TOOMANYREQUEST_1";

    try {
      merchantRestClient.getMerchantDetail(merchantId, initiativeId).block();
      Assertions.fail("Expected retry exhausted exception");
    } catch (Throwable e) {
      Assertions.assertTrue(Exceptions.isRetryExhausted(e));
    }
  }



  @Test
  void getMerchantDetailHttpForbidden() {
    String merchantId = "MERCHANT_FORBIDDEN_1";
    String initiativeId = "INIT_FORBIDDEN_1";

    try {
      merchantRestClient.getMerchantDetail(merchantId, initiativeId).block();
      Assertions.fail("Expected WebClientResponseException.Forbidden");
    } catch (Throwable e) {
      Assertions.assertEquals(WebClientResponseException.Forbidden.class, e.getClass());
    }
  }



  @Test
  void getMerchantDetailUnauthorized() {
    String merchantId = "MERCHANT_UNAUTHORIZED_1";
    String initiativeId = "INIT_UNAUTHORIZED_1";

    try {
      merchantRestClient.getMerchantDetail(merchantId, initiativeId).block();
      Assertions.fail("Expected WebClientResponseException.Unauthorized");
    } catch (Throwable e) {
      Assertions.assertEquals(WebClientResponseException.Unauthorized.class, e.getClass());
    }
  }



  @Test
  void getMerchantDetail_BadRequest_Single() {
    String merchantId = "MERCHANT_BADREQUEST_1";
    String initiativeId = "INIT_BADREQUEST_1";

    MerchantDetailDTO result = merchantRestClient.getMerchantDetail(merchantId, initiativeId).block();
    Assertions.assertNull(result);
  }


}
