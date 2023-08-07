package it.gov.pagopa.idpay.transactions.service.commands.ops;

import com.mongodb.MongoException;
import com.mongodb.client.result.UpdateResult;
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

@ExtendWith(MockitoExtension.class)
class DeleteInitiativeServiceImplTest {
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private AuditUtilities auditUtilitiesMock;

    private DeleteInitiativeService deleteInitiativeService;

    @BeforeEach
    void setUp() {
        deleteInitiativeService = new DeleteInitiativeServiceImpl(
                rewardTransactionRepository,
                auditUtilitiesMock);
    }

    @Test
    void executeOK_delete() {
        String initiativeId = "INITIATIVEID";

        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(initiativeId));
        trx.setChannel("QRCODE");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(initiativeId))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.deleteByInitiativeId(initiativeId))
                .thenReturn(Flux.just(Mockito.mock(RewardTransaction.class)));

        String result = deleteInitiativeService.execute(initiativeId).block();

        Assertions.assertNotNull(result);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findOneByInitiativeId(Mockito.anyString());
        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).deleteByInitiativeId(Mockito.anyString());
        Mockito.verify(rewardTransactionRepository, Mockito.times(0)).findAndRemoveInitiativeOnTransaction(Mockito.anyString());
    }

    @Test
    void executeOK_findAndRemove() {
        String initiativeId = "INITIATIVEID";

        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(initiativeId));
        trx.setChannel("RTD");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(initiativeId))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.findAndRemoveInitiativeOnTransaction(initiativeId))
                .thenReturn(Mono.just(Mockito.mock(UpdateResult.class)));

        String result = deleteInitiativeService.execute(initiativeId).block();

        Assertions.assertNotNull(result);

        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findOneByInitiativeId(Mockito.anyString());
        Mockito.verify(rewardTransactionRepository, Mockito.times(0)).deleteByInitiativeId(Mockito.anyString());
        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).findAndRemoveInitiativeOnTransaction(Mockito.anyString());
    }

    @Test
    void executeError() {
        String initiativeId = "INITIATIVEID";
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setInitiatives(List.of(initiativeId));
        trx.setChannel("QRCODE");

        Mockito.when(rewardTransactionRepository.findOneByInitiativeId(initiativeId))
                .thenReturn(Mono.just(trx));

        Mockito.when(rewardTransactionRepository.deleteByInitiativeId(initiativeId))
                .thenThrow(new MongoException("DUMMY_EXCEPTION"));

        try{
            deleteInitiativeService.execute(initiativeId).block();
            Assertions.fail();
        }catch (Throwable t){
            Assertions.assertTrue(t instanceof  MongoException);
        }
    }
}