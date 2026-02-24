package it.gov.pagopa.idpay.transactions.storage;

public interface ReportBlobService {
    String getFileSignedUrl(String blobPath);
}
