package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.connector.rest.MerchantRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
@AllArgsConstructor
public class RewardBatchDeliveryBatchUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final MerchantRestClient merchantRestClient;
    private final SelfcareInstitutionsRestClient selfcareInstitutionsRestClient;
    private final ErogazioniRestClient erogazioniRestClient;

    public Mono<Void> execute(String initiativeId, List<String> rewardBatchIds) {
        return RewardBatchSharedUtils.processBatchesOrchestrator(
                rewardBatchRepository, initiativeId, rewardBatchIds,
                RewardBatchStatus.APPROVED, this::processSingleBatchDelivery);
    }

    public Mono<RewardBatch> processSingleBatchDelivery(String rewardBatchId, String initiativeId) {

        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
                .filter(rewardBatch -> RewardBatchStatus.APPROVED.equals(rewardBatch.getStatus()))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMap(rewardBatch -> merchantRestClient.getMerchantDetail(rewardBatch.getMerchantId(), initiativeId)
                        .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                                HttpStatus.NOT_FOUND,
                                MERCHANT_NOT_FOUND,
                                ERROR_MESSAGE_MERCHANT_NOT_FOUND.formatted(rewardBatch.getMerchantId(), initiativeId))))
                        .flatMap(merchantDetail -> selfcareInstitutionsRestClient.getInstitutions(merchantDetail.getFiscalCode())
                                .flatMap(institutionList -> {
                                    if (institutionList.getInstitutions() == null || institutionList.getInstitutions().isEmpty()) {
                                        return Mono.error(new ClientExceptionWithBody(
                                                HttpStatus.NOT_FOUND,
                                                MERCHANT_NOT_FOUND_IN_SELFCARE,
                                                ERROR_MESSAGE_MERCHANT_NOT_FOUND_IN_SELFCARE.formatted(merchantDetail.getFiscalCode())));
                                    }
                                    if (institutionList.getInstitutions().size() > 1) {
                                        return Mono.error(new ClientExceptionWithBody(
                                                HttpStatus.CONFLICT,
                                                AMBIGUOUS_MERCHANT_DATA_IN_SELFCARE,
                                                ERROR_MESSAGE_AMBIGUOUS_MERCHANT_DATA_IN_SELFCARE.formatted(merchantDetail.getFiscalCode())));
                                    }

                                    InstitutionDTO institution = institutionList.getInstitutions().getFirst();

                                    DeliveryRequest deliveryRequest = DeliveryRequest.builder()
                                            .id(rewardBatchId)
                                            .anagrafica(AnagraficaDTO.builder()
                                                    .partitaIvaCliente(merchantDetail.getVatNumber())
                                                    .codiceFiscaleCliente(merchantDetail.getFiscalCode())
                                                    .ragioneSocialeIntestatario(merchantDetail.getBusinessName())
                                                    .cap(institution.getZipCode())
                                                    .indirizzo(institution.getAddress())
                                                    .localita(institution.getCity())
                                                    .provincia(institution.getCounty())
                                                    .pec(institution.getDigitalAddress())
                                                    .build())
                                            .erogazione(ErogazioneDTO.builder()
                                                    .idPratica(rewardBatchId)
                                                    .dataAmmissione(rewardBatch.getApprovalDate())
                                                    .ibanBeneficiario(merchantDetail.getIban())
                                                    .importo(rewardBatch.getApprovedAmountCents() / 100.0)
                                                    .intestatarioContoCorrente(merchantDetail.getIbanHolder())
                                                    .build())
                                            .build();

                                    return erogazioniRestClient.postErogazione(deliveryRequest)
                                            .flatMap(outcome -> {
                                                rewardBatch.setDeliveryOutcome(outcome);
                                                if (outcome.isSucceded()) {
                                                    rewardBatch.setStatus(RewardBatchStatus.PENDING_REFUND);
                                                    rewardBatch.setDeliveryDateRequest(LocalDateTime.now());
                                                    log.info("[PROCESS_BATCH] Batch {} delivery succeeded. Status moved to PENDING_REFUND", rewardBatchId);
                                                } else {
                                                    log.warn("[PROCESS_BATCH] Batch {} delivery rejected by server: {}", rewardBatchId, outcome.getMessage());
                                                }

                                                return rewardBatchRepository.save(rewardBatch);
                                            });
                                })));
    }
}

