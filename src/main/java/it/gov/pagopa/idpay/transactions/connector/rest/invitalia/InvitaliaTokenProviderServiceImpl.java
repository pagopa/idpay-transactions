package it.gov.pagopa.idpay.transactions.connector.rest.invitalia;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.config.InvitaliaConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.TokenDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class InvitaliaTokenProviderServiceImpl implements InvitaliaTokenProviderService{
    private final AtomicReference<TokenDTO> cachedToken = new AtomicReference<>();
    private final AtomicReference<Mono<TokenDTO>> ongoingRefresh = new AtomicReference<>();

    private final Integer refreshBeforeExpiryTokenMillis;
    private final InvitaliaTokenRestClient invitaliaTokenRestClient;

    public InvitaliaTokenProviderServiceImpl(InvitaliaConfig invitaliaConfig,
                                             InvitaliaTokenRestClient invitaliaTokenRestClient) {
        this.refreshBeforeExpiryTokenMillis = invitaliaConfig.getToken().getRefreshBeforeExpiry();
        this.invitaliaTokenRestClient = invitaliaTokenRestClient;
    }


    @Override
    public Mono<String> retrieveToken() {
        return Mono.defer(() -> {
            TokenDTO current = cachedToken.get();
            if (current != null && !current.isExpiringSoon(refreshBeforeExpiryTokenMillis)) {
                return Mono.just(current.getAccessToken());
            }

            Mono<TokenDTO> inFlight = ongoingRefresh.get();
            if (inFlight != null) {
                return inFlight.map(TokenDTO::getAccessToken);
            }

            Mono<TokenDTO> refreshMono = invitaliaTokenRestClient.getToken()
                    .doOnNext(cachedToken::set)
                    .doFinally(sig -> ongoingRefresh.compareAndSet(ongoingRefresh.get(), null))
                    .cache();

            if (ongoingRefresh.compareAndSet(null, refreshMono)) {
                return refreshMono.map(TokenDTO::getAccessToken);
            } else {
                return ongoingRefresh.get().map(TokenDTO::getAccessToken);
            }
        });
    }
}
