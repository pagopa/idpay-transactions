package it.gov.pagopa.common.reactive.web.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockUser;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import it.gov.pagopa.common.utils.MemoryAppender;
import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.common.web.exception.ClientException;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.ErrorManager;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.common.web.exception.ServiceException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.regex.Pattern;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ErrorManagerTest.TestController.class, ErrorManager.class})
@WebFluxTest
public class ErrorManagerTest {

    @MockitoSpyBean
    private TestController testController;

    @RestController
    @Slf4j
    static class TestController {
        @GetMapping("/test")
        Mono<String> testEndpoint(){
            return Mono.just("OK");
        }
    }

    @Autowired
    private WebTestClient webTestClient;
    private static MemoryAppender memoryAppender;
    private static ch.qos.logback.classic.Logger errorManagerLogger;
    private static Level previousLoggerLevel;

    @BeforeAll
    static void configureMemoryAppender(){
        memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        memoryAppender.start();
    }

    @AfterAll
    static void stopMemoryAppender() {
        memoryAppender.stop();
    }

    @BeforeEach
    void clearAndAppendMemoryAppender(){
        memoryAppender.reset();
        errorManagerLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ErrorManager.class.getName());
        previousLoggerLevel = errorManagerLogger.getLevel();
        errorManagerLogger.setLevel(Level.DEBUG);
        errorManagerLogger.addAppender(memoryAppender);
    }

    @AfterEach
    void restoreLogger() {
        errorManagerLogger.detachAppender(memoryAppender);
        errorManagerLogger.setLevel(previousLoggerLevel);
    }

    @Test
    void handleExceptionClientExceptionNoBody() {
        Mockito.doThrow(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, "NOTFOUND ClientExceptionNoBody"))
                .when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody().isEmpty();

        checkStackTraceSuppressedLog(memoryAppender,
                "A ClientExceptionNoBody occurred handling request GET /test \\([^)]+\\): HttpStatus 400 BAD_REQUEST - NOTFOUND ClientExceptionNoBody at it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest\\$TestController.testEndpoint\\(ErrorManagerTest.java:[0-9]+\\)");
    }

    @Test
    void handleExceptionClientExceptionWithBody(){
        Mockito.doThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","Error ClientExceptionWithBody"))
                .when(testController).testEndpoint();

        ErrorDTO errorClientExceptionWithBody= new ErrorDTO("Error","Error ClientExceptionWithBody");

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(errorClientExceptionWithBody);


        Mockito.doThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","Error ClientExceptionWithBody", new Throwable()))
                .when(testController).testEndpoint();
        ErrorDTO errorClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable= new ErrorDTO("Error","Error ClientExceptionWithBody");

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(errorClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable);
    }

    @Test
    void handleExceptionClientExceptionTest(){
        ErrorDTO expectedErrorClientException = new ErrorDTO("Error","Something gone wrong");

        Mockito.doThrow(ClientException.class)
                .when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);

        checkStackTraceSuppressedLog(memoryAppender, "A ClientException occurred handling request GET /test \\([^)]+\\): HttpStatus null - null at UNKNOWN");
        memoryAppender.reset();

        Mockito.doThrow(new ClientException(HttpStatus.BAD_REQUEST, "ClientException with httpStatus and message"))
                .when(testController).testEndpoint();
        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);

        checkStackTraceSuppressedLog(memoryAppender, "A ClientException occurred handling request GET /test \\([^)]+\\): HttpStatus 400 BAD_REQUEST - ClientException with httpStatus and message at it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest\\$TestController.testEndpoint\\(ErrorManagerTest.java:[0-9]+\\)");
        memoryAppender.reset();

        Mockito.doThrow(new ClientException(HttpStatus.BAD_REQUEST, "ClientException with httpStatus, message and throwable", new Throwable()))
                .when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);

        checkTechnicalLogs(memoryAppender,
                "Something went wrong handling request GET /test \\([^)]+\\): HttpStatus 400 BAD_REQUEST - ClientException with httpStatus, message and throwable",
                "it.gov.pagopa.common.web.exception.ClientException: ClientException with httpStatus, message and throwable",
                "it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest$TestController.testEndpoint");
    }

    @Test
    void handleExceptionRuntimeException(){
        ErrorDTO expectedErrorDefault = new ErrorDTO("Error","Something gone wrong");

        Mockito.doThrow(new RuntimeException())
                .when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/test").build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDefault);

        checkTechnicalLogs(memoryAppender,
                "Something went wrong handling request GET /test \\([^)]+\\)",
                "java.lang.RuntimeException: null",
                "it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest$TestController.testEndpoint");
    }

    @Test
    void handleExceptionRewardBatchException() {
        RewardBatchException ex = new RewardBatchException(HttpStatus.BAD_REQUEST, "RB_ERROR");

        Mockito.doThrow(ex)
            .when(testController).testEndpoint();

        ErrorDTO expected = new ErrorDTO("RB_ERROR", "RB_ERROR");

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
            .uri("/test")
            .exchange()
            .expectStatus().isBadRequest()
            .expectHeader().contentType("application/json")
            .expectBody(ErrorDTO.class).isEqualTo(expected);

        ILoggingEvent loggedEvent = memoryAppender.getLoggedEvents().get(0);
        assertEquals(Level.INFO, loggedEvent.getLevel());
        assertNull(loggedEvent.getThrowableProxy());
        Assertions.assertTrue(loggedEvent.getFormattedMessage().contains("A RewardBatchException occurred"));
    }

    @Test
    void handleExceptionServiceExceptionWithoutPrintStackTrace() {
        ServiceException serviceException = new ServiceException("BUSINESS_ERROR", "Expected business error");
        Mockito.doThrow(new ClientExceptionWithBody(
                        HttpStatus.BAD_REQUEST,
                        serviceException.getCode(),
                        serviceException.getMessage(),
                        serviceException.isPrintStackTrace(),
                        serviceException))
                .when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/test")
                .exchange()
                .expectStatus().isBadRequest();

        ILoggingEvent loggedEvent = memoryAppender.getLoggedEvents().get(0);
        assertEquals(Level.INFO, loggedEvent.getLevel());
        assertNull(loggedEvent.getThrowableProxy());
        Assertions.assertTrue(loggedEvent.getFormattedMessage().contains("A ClientExceptionWithBody occurred"));
    }

    @Test
    void handleExceptionRewardBatchExceptionWithPrintStackTrace() {
        RewardBatchException exception = new RewardBatchException(
                HttpStatus.INTERNAL_SERVER_ERROR, "RB_TECHNICAL_ERROR", true, null);
        Mockito.doThrow(exception).when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/test")
                .exchange()
                .expectStatus().is5xxServerError();

        checkTechnicalLogs(memoryAppender,
                "Something went wrong handling request GET /test \\([^)]+\\)",
                "it.gov.pagopa.common.web.exception.RewardBatchException: RB_TECHNICAL_ERROR",
                "it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest$TestController.testEndpoint");
    }

    @Test
    void handleExceptionClientExceptionWithPrintStackTraceAndNoCause() {
        ClientException exception = new ClientException(
                HttpStatus.INTERNAL_SERVER_ERROR, "CLIENT_TECHNICAL_ERROR", true, null);
        Mockito.doThrow(exception).when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/test")
                .exchange()
                .expectStatus().is5xxServerError();

        checkTechnicalLogs(memoryAppender,
                "Something went wrong handling request GET /test \\([^)]+\\): HttpStatus 500 INTERNAL_SERVER_ERROR - CLIENT_TECHNICAL_ERROR",
                "it.gov.pagopa.common.web.exception.ClientException: CLIENT_TECHNICAL_ERROR",
                "it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest$TestController.testEndpoint");
    }

    @Test
    void handleExceptionRewardBatchExceptionWithCause() {
        RewardBatchException exception = new RewardBatchException(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "RB_CAUSED_ERROR",
                false,
                new IllegalStateException("Root cause"));
        Mockito.doThrow(exception).when(testController).testEndpoint();

        webTestClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/test")
                .exchange()
                .expectStatus().is5xxServerError();

        checkTechnicalLogs(memoryAppender,
                "Something went wrong handling request GET /test \\([^)]+\\)",
                "it.gov.pagopa.common.web.exception.RewardBatchException: RB_CAUSED_ERROR",
                "it.gov.pagopa.common.reactive.web.exception.ErrorManagerTest$TestController.testEndpoint");
    }

    public static void checkStackTraceSuppressedLog(MemoryAppender memoryAppender, String expectedLoggedMessage) {
        String loggedMessage = memoryAppender.getLoggedEvents().get(0).getFormattedMessage();
        Assertions.assertTrue(Pattern.matches(expectedLoggedMessage, loggedMessage),
                "Unexpected logged message: " + loggedMessage);
    }

    public static void checkTechnicalLogs(MemoryAppender memoryAppender, String expectedLoggedMessageRegexp, String expectedLoggedExceptionMessage, String expectedLoggedExceptionOccurrencePosition) {
        ILoggingEvent errorEvent = memoryAppender.getLoggedEvents().stream()
                .filter(event -> event.getLevel() == Level.ERROR)
                .findFirst()
                .orElseThrow();
        assertNull(errorEvent.getThrowableProxy());
        Assertions.assertTrue(Pattern.matches(expectedLoggedMessageRegexp, errorEvent.getFormattedMessage()),
                "Unexpected logged message: " + errorEvent.getFormattedMessage());

        ILoggingEvent loggedEvent = memoryAppender.getLoggedEvents().stream()
                .filter(event -> event.getLevel() == Level.DEBUG)
                .findFirst()
                .orElseThrow();
        IThrowableProxy loggedException = loggedEvent.getThrowableProxy();
        StackTraceElementProxy loggedExceptionOccurrenceStackTrace = loggedException.getStackTraceElementProxyArray()[0];

        assertEquals(expectedLoggedExceptionMessage,
                loggedException.getClassName() + ": " + loggedException.getMessage());

        assertEquals(expectedLoggedExceptionOccurrencePosition,
                loggedExceptionOccurrenceStackTrace.getStackTraceElement().getClassName() + "." + loggedExceptionOccurrenceStackTrace.getStackTraceElement().getMethodName());
    }

  @Test
  void shouldCreateServiceExceptionWithDefaultValues() {
    String code = "TEST_CODE";
    String message = "Test message";

    ServiceException ex = new ServiceException(code, message);

    assertEquals(code, ex.getCode());
    assertEquals(message, ex.getMessage());
    assertFalse(ex.isPrintStackTrace());
    assertNull(ex.getCause());
  }
}
