package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import it.gov.pagopa.idpay.transactions.connector.rest.InitiativeRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import it.gov.pagopa.idpay.transactions.config.InitiativeNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;

@ExtendWith(MockitoExtension.class)
class InitiativeDataServiceTest {

    private static final String INITIATIVE_ID_1 = "INITIATIVE_001";
    private static final String INITIATIVE_ID_2 = "INITIATIVE_002";

    @Mock
    private InitiativeRestClient initiativeRestClient;

    private InitiativeDataService initiativeDataService;

    @BeforeEach
    void setUp() {
        initiativeDataService = new InitiativeDataService(initiativeRestClient);
    }

    @Test
    void testGetInitiativeData_CacheMiss() {
        LocalDate endDate = LocalDate.parse("2024-12-31");
        InitiativeDetailDTO expectedData = buildInitiativeDetailDTO(
                "Test Initiative", "PUBLISHED", endDate
        );
        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_1))
                .thenReturn(Mono.just(expectedData));

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .assertNext(data -> {
                    assertNotNull(data);
                    assertEquals("Test Initiative", data.getInitiativeName());
                    assertEquals("PUBLISHED", data.getStatus());
                    assertEquals(endDate, data.getFruitionEndDate());
                })
                .verifyComplete();

        verify(initiativeRestClient, times(1))
                .getInitiativeBeneficiaryDetail(INITIATIVE_ID_1);
    }

    @Test
    void testGetInitiativeData_CacheHit() {
        LocalDate endDate = LocalDate.of(2024, 10, 30);
        InitiativeDetailDTO expectedData = buildInitiativeDetailDTO(
                "Cached Initiative", "ACTIVE", endDate
        );
        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_1))
                .thenReturn(Mono.just(expectedData));

        initiativeDataService.getInitiativeData(INITIATIVE_ID_1).block();

        reset(initiativeRestClient);

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .assertNext(data -> {
                    assertNotNull(data);
                    assertEquals("Cached Initiative", data.getInitiativeName());
                    assertEquals("ACTIVE", data.getStatus());
                    assertEquals(endDate, data.getFruitionEndDate());
                })
                .verifyComplete();

        verify(initiativeRestClient, never()).getInitiativeBeneficiaryDetail(anyString());
    }

    @Test
    void testGetInitiativeData_MultipleCaches() {
       InitiativeDetailDTO data1 = buildInitiativeDetailDTO(
                "Initiative 1", "PUBLISHED", LocalDate.parse("2024-12-31")
        );
        InitiativeDetailDTO data2 = buildInitiativeDetailDTO(
                "Initiative 2", "DRAFT", LocalDate.parse("2025-01-31")
        );
        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_1))
                .thenReturn(Mono.just(data1));
        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_2))
                .thenReturn(Mono.just(data2));

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .assertNext(data -> assertEquals("Initiative 1", data.getInitiativeName()))
                .verifyComplete();

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_2))
                .assertNext(data -> assertEquals("Initiative 2", data.getInitiativeName()))
                .verifyComplete();

        verify(initiativeRestClient, times(1))
                .getInitiativeBeneficiaryDetail(INITIATIVE_ID_1);
        verify(initiativeRestClient, times(1))
                .getInitiativeBeneficiaryDetail(INITIATIVE_ID_2);
    }

    @Test
    void testGetInitiativeData_ErrorPropagation() {
        InitiativeNotFoundException testException = new InitiativeNotFoundException(
                "Initiative not found: initiativeId=%s".formatted(INITIATIVE_ID_1)
        );
        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_1))
                .thenReturn(Mono.error(testException));

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .verifyError(InitiativeNotFoundException.class);

        verify(initiativeRestClient, times(1))
                .getInitiativeBeneficiaryDetail(INITIATIVE_ID_1);
    }

    @Test
    void testGetInitiativeData_ErrorDoesNotCache() {
        InitiativeDetailDTO successData = buildInitiativeDetailDTO(
                "Success Initiative", "PUBLISHED", LocalDate.parse("2024-12-31")
        );
        InitiativeNotFoundException firstException = new InitiativeNotFoundException(
                "Initiative not found: initiativeId=%s".formatted(INITIATIVE_ID_1)
        );

        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_1))
                .thenReturn(Mono.error(firstException))
                .thenReturn(Mono.just(successData));

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .verifyError(InitiativeNotFoundException.class);

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .assertNext(data -> {
                    assertNotNull(data);
                    assertEquals("Success Initiative", data.getInitiativeName());
                })
                .verifyComplete();

        verify(initiativeRestClient, times(2))
                .getInitiativeBeneficiaryDetail(INITIATIVE_ID_1);
    }

    @Test
    void testGetInitiativeData_DoOnNextSideEffect() {
        LocalDate endDate = LocalDate.of(2024, 10, 30);
        InitiativeDetailDTO expectedData = buildInitiativeDetailDTO(
                "Side Effect Initiative", "PUBLISHED", endDate
        );
        when(initiativeRestClient.getInitiativeBeneficiaryDetail(INITIATIVE_ID_1))
                .thenReturn(Mono.just(expectedData));

        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .assertNext(data -> {
                    assertNotNull(data);
                    assertEquals("Side Effect Initiative", data.getInitiativeName());
                    assertNotNull(data.getFruitionEndDate());
                })
                .verifyComplete();

        reset(initiativeRestClient);
        StepVerifier.create(initiativeDataService.getInitiativeData(INITIATIVE_ID_1))
                .assertNext(data -> assertEquals("Side Effect Initiative", data.getInitiativeName()))
                .verifyComplete();

        verify(initiativeRestClient, never()).getInitiativeBeneficiaryDetail(anyString());
    }

    private InitiativeDetailDTO buildInitiativeDetailDTO(
            String name, String status, LocalDate fruitionEndDate) {
        return InitiativeDetailDTO.builder()
                .initiativeName(name)
                .status(status)
                .fruitionEndDate(fruitionEndDate)
                .build();
    }
}


