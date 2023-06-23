package it.gov.pagopa.idpay.transactions.connector.pdvuser;

import it.gov.pagopa.idpay.transactions.connector.pdvuser.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.connector.pdvuser.dto.UserInfoPDV;
import reactor.core.publisher.Mono;

public interface UserRestClient {
    Mono<UserInfoPDV> retrieveUserInfo(String userId);
    Mono<FiscalCodeInfoPDV> retrieveFiscalCodeInfo(String fiscalCode);
}
