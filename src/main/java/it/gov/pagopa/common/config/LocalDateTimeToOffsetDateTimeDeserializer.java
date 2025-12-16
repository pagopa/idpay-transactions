package it.gov.pagopa.common.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Configuration
public class LocalDateTimeToOffsetDateTimeDeserializer extends JsonDeserializer<OffsetDateTime> {

    public static final ZoneId ZONEID = ZoneId.of("Europe/Rome");

    @Override
    public OffsetDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String dateString = p.getValueAsString();
        return string2OffsetDateTime(dateString);
    }

    public static OffsetDateTime string2OffsetDateTime(String dateString) {
        if (dateString.contains("+") || dateString.endsWith("Z")) {
            return OffsetDateTime.parse(dateString);
        } else {
            return ZonedDateTime.of(LocalDateTime.parse(dateString), ZONEID).toOffsetDateTime();
        }
    }
}
