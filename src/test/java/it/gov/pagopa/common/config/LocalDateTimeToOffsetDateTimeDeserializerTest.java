package it.gov.pagopa.common.config;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

class LocalDateTimeToOffsetDateTimeDeserializerTest {

    private final LocalDateTimeToOffsetDateTimeDeserializer deserializer =
            new LocalDateTimeToOffsetDateTimeDeserializer();

    @Test
    void deserialize_withOffset_shouldParseCorrectly() throws IOException {
        String date = "2024-01-10T12:30:45+01:00";
        JsonParser parser = new JsonFactory().createParser("\"" + date + "\"");
        parser.nextToken();

        DeserializationContext context =
                new ObjectMapper().getDeserializationContext();

        OffsetDateTime result = deserializer.deserialize(parser, context);

        assertEquals(OffsetDateTime.parse(date), result);
    }

    @Test
    void deserialize_withZ_shouldParseCorrectly() throws IOException {
        String date = "2024-01-10T12:30:45Z";
        JsonParser parser = new JsonFactory().createParser("\"" + date + "\"");
        parser.nextToken();

        DeserializationContext context =
                new ObjectMapper().getDeserializationContext();

        OffsetDateTime result = deserializer.deserialize(parser, context);

        assertEquals(OffsetDateTime.parse(date), result);
    }

    @Test
    void string2OffsetDateTime_withOffset_shouldParseDirectly() {
        String date = "2024-01-10T12:30:45+02:00";

        OffsetDateTime result =
                LocalDateTimeToOffsetDateTimeDeserializer.string2OffsetDateTime(date);

        assertEquals(OffsetDateTime.parse(date), result);
    }

    @Test
    void zoneIdConstant_shouldBeEuropeRome() {
        assertEquals(LocalDateTimeToOffsetDateTimeDeserializer.ZONEID, ZoneId.of("Europe/Rome"));
    }
}

