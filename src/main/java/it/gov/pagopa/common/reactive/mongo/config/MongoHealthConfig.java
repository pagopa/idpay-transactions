package it.gov.pagopa.common.reactive.mongo.config;

import it.gov.pagopa.common.config.CustomReactiveMongoHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

@Configuration
public class MongoHealthConfig {
    @Bean
    public CustomReactiveMongoHealthIndicator customMongoHealthIndicator(ReactiveMongoTemplate mongoTemplate) {
        return new CustomReactiveMongoHealthIndicator(mongoTemplate);
    }
}

