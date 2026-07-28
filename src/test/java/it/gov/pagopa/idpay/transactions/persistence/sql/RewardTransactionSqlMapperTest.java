package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JsonParser;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class RewardTransactionSqlMapperTest {

    private static final String INITIATIVE_ID = "initiative";

    @Test
    void shouldRejectMissingAmbiguousOrBlankInitiatives() {
        RewardTransactionSqlMapper mapper = mapper();

        RewardTransaction missingInitiatives = transaction();
        missingInitiatives.setInitiatives(null);
        assertInvalidInitiative(mapper, missingInitiatives);

        RewardTransaction emptyInitiatives = transaction();
        emptyInitiatives.setInitiatives(List.of());
        assertInvalidInitiative(mapper, emptyInitiatives);

        RewardTransaction blankInitiative = transaction();
        blankInitiative.setInitiatives(List.of(" "));
        assertInvalidInitiative(mapper, blankInitiative);

        RewardTransaction multipleInitiatives = transaction();
        multipleInitiatives.setInitiatives(List.of(INITIATIVE_ID, "another-initiative"));
        assertInvalidInitiative(mapper, multipleInitiatives);
    }

    @Test
    void shouldWrapJsonSerializationFailures() {
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        DatabindException failure = jacksonFailure("serialization failure");
        when(objectMapper.writeValueAsString(any())).thenThrow(failure);
        RewardTransaction transaction = transaction();
        transaction.setRejectionReasons(List.of("reason"));
        RewardTransactionSqlMapper mapper = new RewardTransactionSqlMapper(objectMapper);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> mapper.toEntity(transaction)
        );

        assertEquals("Unable to serialize reward transaction JSON", exception.getMessage());
        assertSame(failure, exception.getCause());
    }

    @Test
    void shouldWrapTypeReferenceJsonDeserializationFailures() {
        RewardTransaction transaction = transaction();
        transaction.setRejectionReasons(List.of("reason"));
        RewardTransactionEntity entity = mapper().toEntity(transaction);
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        DatabindException failure = jacksonFailure("type reference deserialization failure");
        when(objectMapper.readValue(anyString(), any(TypeReference.class))).thenThrow(failure);
        RewardTransactionSqlMapper mapper = new RewardTransactionSqlMapper(objectMapper);

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> mapper.fromEntity(entity)
        );

        assertEquals("Unable to deserialize reward transaction JSON", exception.getMessage());
        assertSame(failure, exception.getCause());
    }

    @Test
    void shouldWrapClassJsonDeserializationFailures() {
        RewardTransaction transaction = transaction();
        transaction.setRefundInfo(new RefundInfo());
        RewardTransactionEntity entity = mapper().toEntity(transaction);
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        DatabindException failure = jacksonFailure("class deserialization failure");
        when(objectMapper.readValue(anyString(), eq(RefundInfo.class))).thenThrow(failure);
        RewardTransactionSqlMapper mapper = new RewardTransactionSqlMapper(objectMapper);

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> mapper.fromEntity(entity)
        );

        assertEquals("Unable to deserialize reward transaction JSON", exception.getMessage());
        assertSame(failure, exception.getCause());
    }

    @Test
    void shouldRejectUnknownPersistedBatchTransactionStatus() {
        RewardTransactionEntity entity = mock(RewardTransactionEntity.class);
        when(entity.initiativeId()).thenReturn(INITIATIVE_ID);
        when(entity.rewardBatchTrxStatus()).thenReturn("UNKNOWN");
        RewardTransactionSqlMapper mapper = mapper();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromEntity(entity));
    }

    @Test
    void shouldRejectUnknownPersistedPointOfSaleType() {
        RewardTransactionEntity entity = mock(RewardTransactionEntity.class);
        when(entity.initiativeId()).thenReturn(INITIATIVE_ID);
        when(entity.pointOfSaleType()).thenReturn("UNKNOWN");
        RewardTransactionSqlMapper mapper = mapper();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromEntity(entity));
    }

    private static RewardTransactionSqlMapper mapper() {
        return new RewardTransactionSqlMapper(JsonMapper.builder().build());
    }

    private static RewardTransaction transaction() {
        return RewardTransaction.builder()
                .id("transaction")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
    }

    private static DatabindException jacksonFailure(String message) {
        return DatabindException.from((JsonParser) null, message);
    }

    private static void assertInvalidInitiative(
            RewardTransactionSqlMapper mapper,
            RewardTransaction transaction
    ) {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> mapper.toEntity(transaction)
        );
        assertEquals("A reward transaction must have exactly one initiative", exception.getMessage());
    }
}
