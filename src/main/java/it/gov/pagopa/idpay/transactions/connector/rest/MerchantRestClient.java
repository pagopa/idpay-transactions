package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import reactor.core.publisher.Mono;

public interface MerchantRestClient {

  Mono<PointOfSaleDTO> getPointOfSale(String merchantId, String pointOfSaleId);

  Mono<MerchantDetailDTO> getMerchantDetail(String merchantId, String initiativeId);


}
