package it.gov.pagopa.common.reactive.mongo;

import io.micrometer.core.instrument.binder.mongodb.MongoMetricsCommandListener;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.gov.pagopa.common.mongo.MongoTestUtilitiesService;
import it.gov.pagopa.common.mongo.config.MongoConfig;
import it.gov.pagopa.common.mongo.singleinstance.AutoConfigureSingleInstanceMongodb;
import it.gov.pagopa.common.reactive.mongo.config.ReactiveMongoConfig;
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
                "de.flapdoodle.mongodb.embedded.version=4.24.0",

                "spring.mongodb.database=idpay",
                "spring.mongodb.config.connectionPool.maxSize: 100",
                "spring.mongodb.config.connectionPool.minSize: 0",
                "spring.mongodb.config.connectionPool.maxWaitTimeMS: 120000",
                "spring.mongodb.config.connectionPool.maxConnectionLifeTimeMS: 0",
                "spring.mongodb.config.connectionPool.maxConnectionIdleTimeMS: 120000",
                "spring.mongodb.config.connectionPool.maxConnecting: 2",
        })
@AutoConfigureSingleInstanceMongodb
@Import({MongoTestUtilitiesService.TestMongoConfiguration.class,
        ReactiveMongoConfig.class,
        SimpleMeterRegistry.class,
        MongoTest.MongoTestConfiguration.class})
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