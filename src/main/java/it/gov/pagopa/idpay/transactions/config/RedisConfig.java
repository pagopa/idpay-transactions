package it.gov.pagopa.idpay.transactions.config;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.PointOfSaleDTO;
import org.springframework.boot.cache.autoconfigure.RedisCacheManagerBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

@Configuration
public class RedisConfig {
  @Bean
  public RedisCacheManagerBuilderCustomizer redisCacheManagerBuilderCustomizer() {

    JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();

    JacksonJsonRedisSerializer<PointOfSaleDTO> serializer = new JacksonJsonRedisSerializer<>(mapper, PointOfSaleDTO.class);

    return builder -> builder
        .withCacheConfiguration("getPointOfSale",
            RedisCacheConfiguration.defaultCacheConfig().entryTtl(Duration.ofDays(1))
                .serializeValuesWith(
                    SerializationPair.fromSerializer(serializer)));
  }
}