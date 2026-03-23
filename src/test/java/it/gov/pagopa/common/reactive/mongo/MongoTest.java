package it.gov.pagopa.common.reactive.mongo;

import io.micrometer.core.instrument.binder.mongodb.MongoMetricsCommandListener;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.gov.pagopa.common.mongo.MongoTestUtilitiesService;
import it.gov.pagopa.common.mongo.config.MongoConfig;
import it.gov.pagopa.common.mongo.singleinstance.AutoConfigureSingleInstanceMongodb;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(SpringExtension.class)
@TestPropertySource(
        properties = {
                "de.flapdoodle.mongodb.embedded.version=4.2.24"
        })
@AutoConfigureSingleInstanceMongodb
@Import({MongoTestUtilitiesService.TestMongoConfiguration.class, SimpleMeterRegistry.class,MongoTest.MongoTestConfiguration.class})
public @interface MongoTest {
    @TestConfiguration
    class MongoTestConfiguration extends MongoConfig {
        @Autowired
        private MongoMetricsCommandListener mongoMetricsCommandListener;

        @Override
        public MongoClientSettingsBuilderCustomizer customizer(MongoDbCustomProperties mongoDbCustomProperties) {
            return builder -> {
                super.customizer(mongoDbCustomProperties).customize(builder);
                builder.addCommandListener(mongoMetricsCommandListener);
            };
        }

    }
}