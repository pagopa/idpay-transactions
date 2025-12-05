package it.gov.pagopa.idpay.transactions.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.cache.RedisCacheManagerBuilderCustomizer;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;


@ExtendWith(MockitoExtension.class)
class RedisConfigTest {

  @InjectMocks
  private RedisConfig redisConfig;

  @Mock
  private RedisCacheManager.RedisCacheManagerBuilder builder;

  @Test
  void testRedisCustomizer() {
    ArgumentCaptor<RedisCacheConfiguration> configCaptor = ArgumentCaptor.forClass(RedisCacheConfiguration.class);

    Mockito.when(builder.withCacheConfiguration(Mockito.eq("getPointOfSale"), configCaptor.capture()))
        .thenReturn(builder);

    RedisCacheManagerBuilderCustomizer customizer = redisConfig.redisCacheManagerBuilderCustomizer();
    customizer.customize(builder);

    Mockito.verify(builder, Mockito.times(1))
        .withCacheConfiguration(Mockito.eq("getPointOfSale"), Mockito.any());

    RedisCacheConfiguration config = configCaptor.getValue();

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getValueSerializationPair());
  }
}
