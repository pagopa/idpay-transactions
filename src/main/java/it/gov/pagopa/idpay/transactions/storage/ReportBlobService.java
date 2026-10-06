package it.gov.pagopa.idpay.transactions.storage;

import reactor.core.publisher.Mono;

public interface ReportBlobService {
    Mono<String> getFileSignedUrl(String blobPath);
}
