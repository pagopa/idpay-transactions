package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Exceptions;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;

//@TestPropertySource(properties = {
//        "logging.level.it.gov.pagopa.transactions.rest.UserRestClientImpl=WARN",
//})
@ContextConfiguration(
        classes = {
                UserRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.pdv.base-url"
        }
)
class UserRestClientImplTest extends BaseWireMockTest {

    @Autowired
    private UserRestClient userRestClient;


    @Test
    void retrieveUserInfoOk() {
        String userId = "USERID_OK_1";

        UserInfoPDV result = userRestClient.retrieveUserInfo(userId).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals("fiscalCode",result.getPii());
    }

    @ParameterizedTest
    @ValueSource(strings = {"USERID_NOTFOUND_1", "USERID_NOTVALID_1", "USERID_BADREQUEST_1"})
    void retrieveUserInfo_NotFound_NotValid_BadRequest(String userId) {

        UserInfoPDV result = userRestClient.retrieveUserInfo(userId).block();
        Assertions.assertNull(result);
    }

    @Test
    void retrieveUserInfoInternalServerError() {
        String userId = "USERID_INTERNALSERVERERROR_1";

        try{
            userRestClient.retrieveUserInfo(userId).block();
            Assertions.fail();
        }catch (Throwable e){
            Assertions.assertTrue(e instanceof WebClientException);
            Assertions.assertEquals(WebClientResponseException.InternalServerError.class,e.getClass());
        }
    }

    @Test
    void retrieveUserInfoTooManyRequest() {
        String userId = "USERID_TOOMANYREQUEST_1";

        try{
            userRestClient.retrieveUserInfo(userId).block();
            Assertions.fail();
        }catch (Throwable e){
            Assertions.assertTrue(Exceptions.isRetryExhausted(e));
        }
    }

    @Test
    void retrieveUserInfoHttpForbidden() {
        String userId = "USERID_FORBIDDEN_1";

        try{
            userRestClient.retrieveUserInfo(userId).block();
            Assertions.fail();
        }catch (Throwable e){
            e.printStackTrace();
            Assertions.assertTrue(e instanceof WebClientException);
            Assertions.assertEquals(WebClientResponseException.Forbidden.class,e.getClass());
        }
    }

    @Test
    void retrieveFiscalCodeInfoOk() {
        String fiscalCode = "FC_OK_1";

        FiscalCodeInfoPDV result = userRestClient.retrieveFiscalCodeInfo(fiscalCode).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals("userId", result.getToken());
    }

    @ParameterizedTest
    @ValueSource(strings = {"FC_NOTFOUND_1", "FC_NOTVALID_1", "FC_BADREQUEST_1"})
    void retrieveFiscalCodeInfo_NotFound_NotValid_BadRequest(String fiscalCode) {
        FiscalCodeInfoPDV result = userRestClient.retrieveFiscalCodeInfo(fiscalCode).block();
        Assertions.assertNull(result);
    }

    @Test
    void retrieveFiscalCodeInfoInternalServerError() {
        String fiscalCode = "FC_INTERNALSERVERERROR_1";

        try{
            userRestClient.retrieveFiscalCodeInfo(fiscalCode).block();
            Assertions.fail();
        }catch (Throwable e){
            Assertions.assertTrue(e instanceof WebClientException);
            Assertions.assertEquals(WebClientResponseException.InternalServerError.class,e.getClass());
        }
    }

    @Test
    void retrieveFiscalCodeInfoTooManyRequest() {
        String fiscalCode = "FC_TOOMANYREQUEST_1";

        try{
            userRestClient.retrieveFiscalCodeInfo(fiscalCode).block();
            Assertions.fail();
        }catch (Throwable e){
            Assertions.assertTrue(Exceptions.isRetryExhausted(e));
        }
    }

    @Test
    void retrieveFiscalCodeInfoHttpForbidden() {
        String fiscalCode = "FC_FORBIDDEN_1";

        try{
            userRestClient.retrieveFiscalCodeInfo(fiscalCode).block();
            Assertions.fail();
        }catch (Throwable e){
            e.printStackTrace();
            Assertions.assertTrue(e instanceof WebClientException);
            Assertions.assertEquals(WebClientResponseException.Forbidden.class,e.getClass());
        }
    }

}