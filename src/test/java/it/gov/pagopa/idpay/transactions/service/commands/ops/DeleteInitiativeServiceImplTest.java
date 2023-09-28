package it.gov.pagopa.idpay.transactions.service.commands.ops;

import com.mongodb.MongoException;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DeleteInitiativeServiceImplTest {
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private AuditUtilities auditUtilitiesMock;

    private DeleteInitiativeService deleteInitiativeService;

    private static final String INITIATIVE_ID = "INITIATIVEID";
    private static final int PAGE_SIZE = 100;
    private static final long DELAY = 1500;
    private static final String TRANSACTION_ID = "TRANSACTION_ID";

    @BeforeEach
    void setUp() {
        deleteInitiativeService = new DeleteInitiativeServiceImpl(
                rewardTransactionRepository,
                auditUtilitiesMock,
                PAGE_SIZE,
                DELAY);
    }

    @Test
    void executeOK_delete() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId(TRANSACTION_ID);
        trx.setInitiatives(List.of(INITIATIVE_ID));
        trx.setChannel("QRCODE");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(INITIATIVE_ID))
                .thenReturn(Mono.just(trx));
        Mockito.when(rewardTransactionRepository.findByInitiativesWithBatch(INITIATIVE_ID, 100))
                .thenReturn(Flux.fromIterable(List.of(trx)));
        Mockito.when(rewardTransactionRepository.deleteById(anyString()))
                .thenReturn(Mono.empty());

        String result = deleteInitiativeService.execute(INITIATIVE_ID).block();

        Assertions.assertNotNull(result);
        verify(rewardTransactionRepository, Mockito.times(1)).findOneByInitiativeId(anyString());
        verify(rewardTransactionRepository, Mockito.times(1)).deleteById(anyString());
        verify(rewardTransactionRepository, Mockito.times(0)).removeInitiativeOnTransaction(anyString(), eq(INITIATIVE_ID));
    }

    @Test
    void executeOK_findAndRemove() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId(TRANSACTION_ID);
        trx.setInitiatives(List.of(INITIATIVE_ID));
        trx.setChannel("RTD");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(INITIATIVE_ID))
                .thenReturn(Mono.just(trx));
        Mockito.when(rewardTransactionRepository.findByInitiativesWithBatch(INITIATIVE_ID, 100))
                .thenReturn(Flux.fromIterable(List.of(trx)));
        Mockito.when(rewardTransactionRepository.removeInitiativeOnTransaction(anyString(), anyString()))
                .thenReturn(Mono.empty());

        String result = deleteInitiativeService.execute(INITIATIVE_ID).block();

        Assertions.assertNotNull(result);
        verify(rewardTransactionRepository, Mockito.times(1)).findOneByInitiativeId(anyString());
        verify(rewardTransactionRepository, Mockito.times(0)).deleteById(anyString());
        verify(rewardTransactionRepository, Mockito.times(1)).removeInitiativeOnTransaction(anyString(), eq(INITIATIVE_ID));
    }

    @Test
    void executeError() {
        String initiativeId = "INITIATIVEID";
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(initiativeId));
        trx.setChannel("QRCODE");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(initiativeId))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.findByInitiativesWithBatch(eq(initiativeId), Mockito.anyInt()))
                .thenReturn(Flux.just(trx));

        Mockito.when(rewardTransactionRepository.deleteById(trx.getId()))
                .thenThrow(new MongoException("DUMMY_EXCEPTION"));

        try{
            deleteInitiativeService.execute(initiativeId).block();
            Assertions.fail();
        }catch (Throwable t){
            Assertions.assertTrue(t instanceof  MongoException);
        }
    }
}