package it.gov.pagopa.idpay.transactions.dto.batch;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldNameConstants;

@FieldNameConstants
@Data
@AllArgsConstructor
@Builder
public class UpdateStatusBatchDTO {
    private String initiativeId;
    private String merchantId;
    private String batchId;

}
