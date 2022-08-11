package it.gov.pagopa.idpay.transactions;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.process.runtime.Executable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.AutoConfigureDataMongo;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.ReflectionUtils;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;

@SpringBootTest
@TestPropertySource(
        properties = {
                //region mongodb
                "logging.level.org.mongodb.driver=WARN",
                "logging.level.org.springframework.boot.autoconfigure.mongo.embedded=WARN",
                "spring.mongodb.embedded.version=4.0.21",
                //endregion
        })
@AutoConfigureDataMongo
public abstract class BaseIntegrationTest {

    @Autowired
    private MongodExecutable embeddedMongoServer;

    @Autowired
    protected ObjectMapper objectMapper;

    @BeforeAll
    public static void setDefaultTimezone() {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Europe/Rome")));
    }

    @PostConstruct
    public void logEmbeddedServerConfig() throws NoSuchFieldException, UnknownHostException {
        Field mongoEmbeddedServerConfigField = Executable.class.getDeclaredField("config");
        mongoEmbeddedServerConfigField.setAccessible(true);
        MongodConfig mongodConfig = (MongodConfig) ReflectionUtils.getField(mongoEmbeddedServerConfigField, embeddedMongoServer);
        Net mongodNet = Objects.requireNonNull(mongodConfig).net();

        System.out.printf("""
                        ************************
                        Embedded mongo: %s
                        ************************
                        """,
                "mongo://%s:%s".formatted(mongodNet.getServerAddress().getHostAddress(), mongodNet.getPort())
                );
    }

    protected static void waitFor(Callable<Boolean> test, Supplier<String> buildTestFailureMessage, int maxAttempts, int millisAttemptDelay) {
        try {
            await()
                    .pollInterval(millisAttemptDelay, TimeUnit.MILLISECONDS)
                    .atMost((long) maxAttempts * millisAttemptDelay, TimeUnit.MILLISECONDS)
                    .until(test);
        } catch (RuntimeException e) {
            Assertions.fail(buildTestFailureMessage.get(), e);
        }
    }

}
