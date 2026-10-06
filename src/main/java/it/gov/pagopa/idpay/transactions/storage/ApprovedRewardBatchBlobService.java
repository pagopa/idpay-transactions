package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;
import reactor.core.publisher.Mono;

import java.io.InputStream;

public interface ApprovedRewardBatchBlobService {

    Mono<String> getFileSignedUrl(String blobPath);

    Mono<Response<BlockBlobItem>> upload(InputStream inputStream, String destination, String contentType);
}
