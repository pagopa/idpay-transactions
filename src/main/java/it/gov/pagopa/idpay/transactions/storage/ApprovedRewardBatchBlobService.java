package it.gov.pagopa.idpay.transactions.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.BlockBlobItem;

import java.io.InputStream;

public interface ApprovedRewardBatchBlobService {

    String getFileSignedUrl(String blobPath);

    Response<BlockBlobItem> upload(InputStream inputStream, String destination, String contentType);
}
