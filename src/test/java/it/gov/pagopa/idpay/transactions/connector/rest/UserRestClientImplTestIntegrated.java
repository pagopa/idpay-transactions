package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;

/**
 * See confluence page: <a href="https://pagopa.atlassian.net/wiki/spaces/IDPAY/pages/615974424/Secrets+UnitTests">Secrets for UnitTests</a>
 */
@SuppressWarnings({"squid:S3577", "NewClassNamingConvention"}) // suppressing class name not match alert: we are not using the Test suffix in order to let not execute this test by default maven configuration because it depends on properties not pushable. See
@TestPropertySource(locations = {
        "classpath:/secrets/appPdv.properties",
},
        properties = {
                "app.pdv.base-url=https://api.uat.tokenizer.pdv.pagopa.it/tokenizer/v1"
        })
class UserRestClientImplTestIntegrated extends BaseIntegrationTest {

    @Autowired
    private UserRestClient userRestClient;

    @Value("${app.pdv.userIdOk:2c2a70cb-d0dd-4c1d-89ca-45dc93798fff}")
    private String userIdOK;
    @Value("${app.pdv.userFiscalCodeExpected:RNZPMP80A44X000M}")
    private String fiscalCodeOKExpected;
    @Value("${app.pdv.userIdNotFound:02105b50-9a81-4cd2-8e17-6573ebb09195}")
    private String userIdNotFound;
    @Value("${app.pdv.userIdNotFound:02105b50-9a81-4cd2-8e17-6573ebb09195AAAA}")
    private String userIdNotValid;

    @Test
    void retrieveUserInfoOk() {
        UserInfoPDV result = userRestClient.retrieveUserInfo(userIdOK).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(fiscalCodeOKExpected, result.getPii());

    }

    @Test
    void retrieveUserInfoNotFound() {
        UserInfoPDV result = userRestClient.retrieveUserInfo(userIdNotFound).block();
        Assertions.assertNull(result);
    }

    @Test
    void retrieveUserInfoNotValid() {
        UserInfoPDV result = userRestClient.retrieveUserInfo(userIdNotValid).block();
        Assertions.assertNull(result);
    }

    @Test
    void retrieveFiscalCodeInfoOk() {
        FiscalCodeInfoPDV result = userRestClient.retrieveFiscalCodeInfo(fiscalCodeOKExpected).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(userIdOK, result.getToken());

    }

}