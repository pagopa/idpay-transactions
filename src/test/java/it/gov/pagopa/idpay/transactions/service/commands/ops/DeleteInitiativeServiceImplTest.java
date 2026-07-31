package it.gov.pagopa.idpay.transactions.service.commands.ops;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.persistence.port.InitiativeTransactionDeletionPort;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class DeleteInitiativeServiceImplTest {

    @Mock private InitiativeTransactionDeletionPort deletionPort;
    @Mock private AuditUtilities auditUtilities;
    private DeleteInitiativeServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new DeleteInitiativeServiceImpl(deletionPort, auditUtilities);
    }

    @Test
    void deletesOnlyTheRequestedInitiativeAndAuditsDeletedRows() {
        when(deletionPort.deleteTransactions("initiative")).thenReturn(Mono.just(2L));

        StepVerifier.create(service.execute("initiative"))
                .expectNext("initiative")
                .verifyComplete();

        verify(deletionPort).deleteTransactions("initiative");
        verify(auditUtilities).logTransactionsDeleted(2L, "initiative");
    }

    @Test
    void propagatesDeletionFailureWithoutAuditing() {
        IllegalStateException failure = new IllegalStateException("database unavailable");
        when(deletionPort.deleteTransactions("initiative")).thenReturn(Mono.error(failure));

        StepVerifier.create(service.execute("initiative"))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }
}
