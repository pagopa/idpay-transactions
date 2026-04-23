package it.gov.pagopa.idpay.transactions.connector.rest.invitalia;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.config.InvitaliaConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.TokenDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InvitaliaTokenProviderServiceImplTest {

    @Mock
    private InvitaliaConfig invitaliaConfig;

    @Mock
    private InvitaliaTokenRestClient invitaliaTokenRestClient;

    private InvitaliaTokenProviderService service;

    private final Clock clock = Clock.fixed(Instant.parse("2026-04-03T10:00:00Z"), ZoneOffset.UTC);

    @BeforeEach
    void setUp() {
        InvitaliaConfig.Token tokenMock = mock(InvitaliaConfig.Token.class);
        when(invitaliaConfig.getToken()).thenReturn(tokenMock);
        when(tokenMock.getRefreshBeforeExpiry()).thenReturn(60000);
        service = new InvitaliaTokenProviderServiceImpl(invitaliaConfig, invitaliaTokenRestClient, clock);
    }

    @Test
    void retrieveToken_inCache() {
        TokenDTO tokenCached = new TokenDTO("TOKEN", 360,clock);

        AtomicReference<TokenDTO> cached = (AtomicReference<TokenDTO>) ReflectionTestUtils.getField(service, "cachedToken");
        cached.set(tokenCached);

        String result = service.retrieveToken().block();

        assertNotNull(result);
        assertEquals(tokenCached.getAccessToken(), result);
    }

    @Test
    void retrieveToken_notInCache() {
        TokenDTO token = new TokenDTO("TOKEN", 360,clock);
        when(invitaliaTokenRestClient.getToken()).thenReturn(Mono.just(token));

        String result = service.retrieveToken().block();

        assertNotNull(result);
        assertEquals(token.getAccessToken(), result);

        AtomicReference<TokenDTO> cached = ( AtomicReference<TokenDTO>)ReflectionTestUtils.getField(service, "cachedToken");
        TokenDTO tokenCached = cached.get();
        assertNotNull(tokenCached);
        assertEquals(token, tokenCached);
    }

    @Test
    void retrieveToken_ongoingInCache() {
        TokenDTO token = new TokenDTO("TOKEN", 360,clock);
        AtomicReference<Mono<TokenDTO>> ongoingCache = (AtomicReference<Mono<TokenDTO>>) ReflectionTestUtils.getField(service, "ongoingRefresh");
        ongoingCache.set(Mono.just(token));

        String result = service.retrieveToken().block();

        assertNotNull(result);
        assertEquals(token.getAccessToken(), result);
    }

    @Test
    void retrieveToken_ongoingInCacheParallels() {
        TokenDTO tokenCached = new TokenDTO("TOKEN", 0,clock);
        AtomicReference<TokenDTO> cached = (AtomicReference<TokenDTO>) ReflectionTestUtils.getField(service, "cachedToken");
        cached.set(tokenCached);

        TokenDTO token = new TokenDTO("TOKEN1", 360,clock);
        when(invitaliaTokenRestClient.getToken()).thenReturn(Mono.just(token));

        String result = service.retrieveToken().block();

        assertNotNull(result);
        assertEquals(token.getAccessToken(), result);
    }

}