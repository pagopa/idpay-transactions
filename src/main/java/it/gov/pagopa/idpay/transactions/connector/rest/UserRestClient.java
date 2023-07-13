package it.gov.pagopa.idpay.transactions.connector.rest;


import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import reactor.core.publisher.Mono;

public interface UserRestClient {
    Mono<UserInfoPDV> retrieveUserInfo(String userId);
    Mono<FiscalCodeInfoPDV> retrieveFiscalCodeInfo(String fiscalCode);
}