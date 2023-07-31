package it.gov.pagopa.common.reactive.mongo.retry;

import it.gov.pagopa.common.reactive.mongo.retry.exception.MongoRequestRateTooLargeRetryExpiredException;
import it.gov.pagopa.common.reactive.web.ReactiveRequestContextFilter;
import it.gov.pagopa.common.reactive.web.ReactiveRequestContextHolder;
import it.gov.pagopa.common.web.exception.ErrorManager;
import it.gov.pagopa.common.web.exception.MongoExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {
        ReactiveRequestContextFilter.class,
        MongoRequestRateTooLargeRetryWhenNotControllerAspect.class,
        ErrorManager.class,
        MongoExceptionHandler.class,

        MongoRequestRateTooLargeRetryWhenNoControllerTest.TestController.class,
        MongoRequestRateTooLargeRetryWhenNoControllerTest.TestRepository.class
})
@WebFluxTest
class MongoRequestRateTooLargeRetryWhenNoControllerTest {

    @Value("${mongo.request-rate-too-large.max-retry:3}")
    private int maxRetry;
    @Value("${mongo.request-rate-too-large.max-millis-elapsed:0}")
    private int maxMillisElapsed;

    @SpyBean
    private TestRepository testRepositorySpy;

    private static int[] counter;

    @BeforeEach
    void init() {
        counter = new int[]{0};
    }

    @RestController
    @Slf4j
    static class TestController {

        @Autowired
        private TestRepository repository;

        @GetMapping("/testMono")
        Mono<String> testMonoEndpoint() {
            return ReactiveRequestContextHolder.getRequest()
                    .doOnNext(r -> {
                        System.out.println("OK");
                        Assertions.assertEquals("/testMono", r.getURI().getPath());
                    })
                    .flatMap(x -> buildNestedRepositoryMonoMethodInvoke(repository));
        }

        @GetMapping("/testFlux")
        Flux<LocalDateTime> testFluxEndpoint() {
            return ReactiveRequestContextHolder.getRequest()
                    .doOnNext(r -> {
                        System.out.println("OK");
                        Assertions.assertEquals("/testFlux", r.getURI().getPath());
                    })
                    .flatMapMany(x -> buildNestedFluxChain(repository));
        }

        static Mono<String> buildNestedRepositoryMonoMethodInvoke(TestRepository repository) {
            return Mono.just("")
                    .flatMap(x ->
                            Mono.delay(Duration.ofMillis(5))
                                    .flatMap(y -> repository.testMono())
                    );
        }

        static Flux<LocalDateTime> buildNestedFluxChain(TestRepository repository) {
            return Flux.just("")
                    .flatMap(x ->
                            Mono.delay(Duration.ofMillis(5))
                                    .flatMapMany(y -> repository.testFlux())
                    );
        }
    }

    @Service
    static class TestRepository {
        public Mono<String> testMono() {
            return Mono.defer(() -> {
                counter[0]++;
                return Mono.error(MongoRequestRateTooLargeRetryerTest.buildRequestRateTooLargeMongodbException());
            });
        }

        public Flux<LocalDateTime> testFlux() {
            return Flux.defer(() -> {
                counter[0]++;
                return Flux.error(MongoRequestRateTooLargeRetryerTest.buildRequestRateTooLargeMongodbException());
            });
        }
    }

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void testController_MonoMethod() {
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/testMono").build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
                .expectBody().json("{\"code\":\"TOO_MANY_REQUESTS\",\"message\":\"TOO_MANY_REQUESTS\"}");

        Assertions.assertEquals(1, counter[0]);
    }

    @Test
    void testController_FluxMethod() {
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/testFlux").build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
                .expectBody().json("{\"code\":\"TOO_MANY_REQUESTS\",\"message\":\"TOO_MANY_REQUESTS\"}");

        Assertions.assertEquals(1, counter[0]);
    }

    @Test
    void testNoController_MonoMethod() {
        testNoController(TestController.buildNestedRepositoryMonoMethodInvoke(testRepositorySpy));
    }

    @Test
    void testNoController_FluxMethod() {
        testNoController(TestController.buildNestedFluxChain(testRepositorySpy).collectList());
    }

    private void testNoController(Mono<?> mono) {
        try {
            mono.block();
            Assertions.fail("Expected exception");
        } catch (MongoRequestRateTooLargeRetryExpiredException e) {
            Assertions.assertEquals(maxRetry + 1, e.getCounter());
            Assertions.assertEquals(maxRetry, e.getMaxRetry());
            Assertions.assertEquals(maxMillisElapsed, e.getMaxMillisElapsed());
            Assertions.assertTrue(e.getMillisElapsed() > 0);
        }

        Assertions.assertEquals(counter[0], maxRetry + 1);
    }
}