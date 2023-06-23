package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MerchantTransactionsListDTO {
    private List<MerchantTransactionDTO> content;
    private int pageNo;
    private int pageSize;
    private int totalElements;
    private int totalPages;
}
