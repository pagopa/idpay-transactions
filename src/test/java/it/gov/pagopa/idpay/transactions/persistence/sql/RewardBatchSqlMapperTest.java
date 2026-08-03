package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import org.jooq.JSONB;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class RewardBatchSqlMapperTest {

    private final RewardBatchSqlMapper mapper = new RewardBatchSqlMapper(JsonMapper.builder().build());

    @Test
    void shouldMapDeliveryOutcomeToJsonbAndKeepNullOutcomesNull() {
        DeliveryOutcomeDTO deliveryOutcome = DeliveryOutcomeDTO.builder()
                .succeded(true)
                .message("accepted")
                .build();

        JSONB jsonb = mapper.toJooqJsonb(deliveryOutcome);

        assertTrue(jsonb.data().contains("\"succeded\":true"));
        assertTrue(jsonb.data().contains("\"message\":\"accepted\""));
        assertNull(mapper.toJooqJsonb(null));
    }
}
