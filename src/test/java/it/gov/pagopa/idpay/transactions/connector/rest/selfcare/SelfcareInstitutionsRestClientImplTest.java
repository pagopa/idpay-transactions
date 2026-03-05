package it.gov.pagopa.idpay.transactions.connector.rest.selfcare;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.exception.SelfcareConnectingErrorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import java.util.Map;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ContextConfiguration(
        classes = {
                SelfcareInstitutionsRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.selfcare.institutions-url"
        }
)
class SelfcareInstitutionsRestClientImplTest extends BaseWireMockTest {

    @Autowired
    SelfcareInstitutionsRestClient selfcareInstitutionsRestClient;

    @Test
    void getInstitutionInfo_ok(){
        String taxCode = "TAXCODE_OK_1";
        InstitutionList result = selfcareInstitutionsRestClient.getInstitutions(taxCode).block();

        assertNotNull(result);
    }

    @Test
    void getInstitutionInfo_koInternalServerError() {
        String taxCode = "TAXCODE_KO_INTERNAL_1";

        try{
            selfcareInstitutionsRestClient.getInstitutions(taxCode).block();
            Assertions.fail();
        } catch (Throwable e){
            Assertions.assertInstanceOf(SelfcareConnectingErrorException.class, e);
        }
    }

    @Test
    void getInstitutionInfo_ok_cached(){
        String taxCode = "TAXCODE_CACHED";
        InstitutionList expected = Mockito.mock(InstitutionList.class);
        Map<String, Mono<InstitutionList>> cache = (Map<String, Mono<InstitutionList>>) ReflectionTestUtils.getField(selfcareInstitutionsRestClient, "institutionsCache");
        assertNotNull(cache);
        cache.put(taxCode, Mono.just(expected));

        InstitutionList result = selfcareInstitutionsRestClient.getInstitutions(taxCode).block();

        assertNotNull(result);
        assertEquals(expected, result);
    }

}