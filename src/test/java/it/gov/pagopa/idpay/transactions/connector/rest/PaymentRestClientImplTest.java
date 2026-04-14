package it.gov.pagopa.idpay.transactions.connector.rest;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Exceptions;
import reactor.test.StepVerifier;

@ContextConfiguration(
        classes = {
                PaymentRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.payment.base-url"
        }
)
class PaymentRestClientImplTest extends BaseWireMockTest {

    private static final String MERCHANT_ID = "MERCHANT_1";
    private static final String ACQUIRER_ID = "ACQUIRER_1";
    private static final String POINT_OF_SALE_ID = "POS_1";

    @Autowired
    private PaymentRestClient paymentRestClient;

    @Test
    void cancelTransaction_Ok_NotFount_BadRequest() {
        // responses not throwing exception

        StepVerifier.create(paymentRestClient.cancelTransaction("TRX_OK_1", MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID))
                .verifyComplete();
        StepVerifier.create(paymentRestClient.cancelTransaction("TRX_NOTFOUND_1", MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID))
                .verifyComplete();
        StepVerifier.create(paymentRestClient.cancelTransaction("TRX_BADREQUEST_1", MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID))
                .verifyComplete();
    }

    @Test
    void cancelTransaction_InternalServerError() {
        String transactionId = "TRX_INTERNALSERVERERROR_1";

        try {
            paymentRestClient.cancelTransaction(transactionId, MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID).block();
            Assertions.fail("Expected WebClientResponseException.InternalServerError");
        } catch (Throwable e) {
            Assertions.assertEquals(WebClientResponseException.InternalServerError.class, e.getClass());
        }
    }

    @Test
    void cancelTransaction_TooManyRequest() {
        String transactionId = "TRX_TOOMANYREQUEST_1";

        try {
            paymentRestClient.cancelTransaction(transactionId, MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID).block();
            Assertions.fail("Expected retry exhausted exception");
        } catch (Throwable e) {
            Assertions.assertTrue(Exceptions.isRetryExhausted(e));
        }
    }

    @Test
    void cancelTransaction_Forbidden() {
        String transactionId = "TRX_FORBIDDEN_1";

        try {
            paymentRestClient.cancelTransaction(transactionId, MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID).block();
            Assertions.fail("Expected WebClientResponseException.Forbidden");
        } catch (Throwable e) {
            Assertions.assertEquals(WebClientResponseException.Forbidden.class, e.getClass());
        }
    }

    @Test
    void cancelTransaction_Unauthorized() {
        String transactionId = "TRX_UNAUTHORIZED_1";

        try {
            paymentRestClient.cancelTransaction(transactionId, MERCHANT_ID, ACQUIRER_ID, POINT_OF_SALE_ID).block();
            Assertions.fail("Expected WebClientResponseException.Unauthorized");
        } catch (Throwable e) {
            Assertions.assertEquals(WebClientResponseException.Unauthorized.class, e.getClass());
        }
    }
}

