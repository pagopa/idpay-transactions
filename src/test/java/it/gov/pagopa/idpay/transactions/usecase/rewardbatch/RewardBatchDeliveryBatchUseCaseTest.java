package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.MerchantDetailDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardBatchDeliveryBatchUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private MerchantRestClient merchantRestClient;
    @Mock private SelfcareInstitutionsRestClient selfcareInstitutionsRestClient;
    @Mock private ErogazioniRestClient erogazioniRestClient;

    private RewardBatchDeliveryBatchUseCase useCase;
    private RewardBatchDeliveryBatchUseCase useCaseSpy;

    private static final String BATCH_ID = "BATCH_ID";
    private static final String BATCH_ID_2 = "BATCH_ID_2";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";

    @BeforeEach
    void setup() {
        useCase = new RewardBatchDeliveryBatchUseCase(rewardBatchRepository, merchantRestClient, selfcareInstitutionsRestClient, erogazioniRestClient);
        useCaseSpy = spy(useCase);
    }

    @Test
    void execute_shouldContinueOnSingleBatchError() {
        doReturn(Mono.error(new RuntimeException("Error Batch 1"))).when(useCaseSpy).processSingleBatchDelivery(eq(BATCH_ID), anyString());
        doReturn(Mono.just(new RewardBatch())).when(useCaseSpy).processSingleBatchDelivery(eq(BATCH_ID_2), anyString());
        StepVerifier.create(useCaseSpy.execute(INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2))).verifyComplete();
        verify(useCaseSpy).processSingleBatchDelivery(BATCH_ID, INITIATIVE_ID);
        verify(useCaseSpy).processSingleBatchDelivery(BATCH_ID_2, INITIATIVE_ID);
    }

    @Test
    void execute_Success() {
        String batchId = "BATCH_1";
        String merchantId = "MERCHANT_1";
        String fiscalCode = "FISCAL_123";
        RewardBatch batch = new RewardBatch(); batch.setId(batchId); batch.setMerchantId(merchantId); batch.setStatus(RewardBatchStatus.APPROVED); batch.setApprovedAmountCents(1000L);
        MerchantDetailDTO merchantDetail = new MerchantDetailDTO(); merchantDetail.setFiscalCode(fiscalCode); merchantDetail.setVatNumber("VAT_123");
        InstitutionDTO inst = new InstitutionDTO(); inst.setZipCode("00100"); inst.setDigitalAddress("pec@test.it");
        InstitutionList instList = new InstitutionList(List.of(inst));
        when(rewardBatchRepository.findRewardBatchById(batchId)).thenReturn(Mono.just(batch));
        when(merchantRestClient.getMerchantDetail(merchantId, INITIATIVE_ID)).thenReturn(Mono.just(merchantDetail));
        when(selfcareInstitutionsRestClient.getInstitutions(fiscalCode)).thenReturn(Mono.just(instList));
        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class))).thenReturn(Mono.empty());
        StepVerifier.create(useCaseSpy.execute(INITIATIVE_ID, List.of(batchId))).verifyComplete();
        verify(erogazioniRestClient).postErogazione(argThat(req -> req.getId().equals(batchId) && req.getAnagrafica().getCap().equals("00100")));
    }

    @Test
    void execute_Fail_MultipleInstitutions() {
        String batchId = "BATCH_1";
        String fiscalCode = "FISCAL_123";
        RewardBatch batch = new RewardBatch(); batch.setId(batchId); batch.setMerchantId("M1"); batch.setStatus(RewardBatchStatus.APPROVED);
        MerchantDetailDTO merchantDetail = new MerchantDetailDTO(); merchantDetail.setFiscalCode(fiscalCode); merchantDetail.setVatNumber("VAT123"); merchantDetail.setBusinessName("Business"); merchantDetail.setIban("IT00TEST"); merchantDetail.setIbanHolder("Holder");
        InstitutionList instList = new InstitutionList(List.of(new InstitutionDTO(), new InstitutionDTO()));
        when(rewardBatchRepository.findRewardBatchById(batchId)).thenReturn(Mono.just(batch));
        when(merchantRestClient.getMerchantDetail(anyString(), anyString())).thenReturn(Mono.just(merchantDetail));
        when(selfcareInstitutionsRestClient.getInstitutions(fiscalCode)).thenReturn(Mono.just(instList));
        StepVerifier.create(useCase.execute(INITIATIVE_ID, List.of(batchId))).verifyComplete();
        verify(erogazioniRestClient, never()).postErogazione(any());
    }
}

