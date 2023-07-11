package it.gov.pagopa.idpay.transactions.test.utils;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class RestTestUtils {

    public static final String TRUSTSTORE_PATH = "src/test/resources/keystore.p12";
    public static final String TRUSTSTORE_KO_PATH = "src/test/resources/keystore2.p12";
    public static boolean USE_TRUSTSTORE_KO = false;

    public static WireMockConfiguration getWireMockConfiguration(){
        return wireMockConfig()
                .dynamicPort()
                .httpsPort(0)
                .needClientAuth(true)
                .trustStorePath(USE_TRUSTSTORE_KO ? TRUSTSTORE_KO_PATH : TRUSTSTORE_PATH)
                .trustStorePassword("idpay")
                .usingFilesUnderClasspath("src/test/resources/stub");
    }
}
