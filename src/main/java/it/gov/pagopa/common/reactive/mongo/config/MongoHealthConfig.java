package it.gov.pagopa.common.reactive.mongo.config;

import it.gov.pagopa.common.mongo.config.CustomMongoHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

@Configuration
public class MongoHealthConfig {
    @Bean
    public CustomMongoHealthIndicator customMongoHealthIndicator(ReactiveMongoTemplate mongoTemplate) {
        return new CustomMongoHealthIndicator(mongoTemplate);
    }
}

