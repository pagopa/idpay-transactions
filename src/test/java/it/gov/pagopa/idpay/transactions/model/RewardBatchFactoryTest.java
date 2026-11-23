package it.gov.pagopa.idpay.transactions.model;

import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class RewardBatchFactoryTest {

    private static final Pattern OBJECT_ID_PATTERN = Pattern.compile("^[0-9a-f]{24}$");

    @Test
    void shouldCreateBatchWithObjectIdFormattedIdentifier() {
        RewardBatch batch = RewardBatchFactory.create(
                "initiative-1",
                "merchant-1",
                PosType.PHYSICAL,
                "2026-07",
                "Business"
        );

        assertTrue(
                OBJECT_ID_PATTERN.matcher(batch.getId()).matches(),
                "id '%s' does not match ^[0-9a-f]{24}$".formatted(batch.getId())
        );
    }
}
