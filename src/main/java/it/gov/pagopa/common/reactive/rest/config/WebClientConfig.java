package it.gov.pagopa.common.reactive.rest.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class WebClientConfig {
    private final int connectTimeoutMillis;
    private final int responseTimeoutMillis;
    private final int readTimeoutHandlerMillis;
    private final int writeTimeoutHandlerMillis;

    public WebClientConfig(
            @Value("${app.web-client.connect.timeout.millis}") int connectTimeoutMillis,
            @Value("${app.web-client.response.timeout}") int responseTimeoutMillis,
            @Value("${app.web-client.read.handler.timeout}") int readTimeoutHandlerMillis,
            @Value("${app.web-client.write.handler.timeout}") int writeTimeoutHandlerMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.responseTimeoutMillis = responseTimeoutMillis;
        this.readTimeoutHandlerMillis = readTimeoutHandlerMillis;
        this.writeTimeoutHandlerMillis = writeTimeoutHandlerMillis;
    }

    @Bean
    public HttpClient httpClientConfig() {
        return HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .responseTimeout(Duration.ofMillis(responseTimeoutMillis))
                .doOnConnected(connection -> connection
                        .addHandlerLast(new ReadTimeoutHandler(readTimeoutHandlerMillis, TimeUnit.MILLISECONDS))
                        .addHandlerLast(new WriteTimeoutHandler(writeTimeoutHandlerMillis, TimeUnit.MILLISECONDS))
                );
    }

    @Bean
    public WebClient.Builder webClientConfigure() {
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClientConfig()));
    }
}