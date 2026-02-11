package it.gov.pagopa.idpay.transactions.model;

import lombok.*;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldNameConstants
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "merchant")
public class Merchant {

    @Id
    private String id;
    private String businessName;
}
