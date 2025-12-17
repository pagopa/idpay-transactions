package it.gov.pagopa.common.config;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Configuration
public class OffsetDateTimeToLocalDateTimeSerializer extends StdSerializer<OffsetDateTime> {

    public static final ZoneId ZONEID = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public OffsetDateTimeToLocalDateTimeSerializer() {
        super(OffsetDateTime.class);
    }

    @Override
    public void serialize(OffsetDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        if (value != null) {
            String formattedDate = value.atZoneSameInstant(ZONEID).toLocalDateTime().format(FORMATTER);
            gen.writeString(formattedDate);
        } else {
            gen.writeNull();
        }
    }
}
