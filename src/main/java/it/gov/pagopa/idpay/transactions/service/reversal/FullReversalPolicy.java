package it.gov.pagopa.idpay.transactions.service.reversal;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.TRANSACTION_STATUS_NOT_ALLOWED;

public class FullReversalPolicy implements ReversalPolicy {

  private static final String SCOPE = "api:storno:full";

  @Override
  public boolean supports(List<String> scopes) {
    return scopes != null && scopes.stream().anyMatch(s -> SCOPE.equals(s));
  }

  @Override
  public Mono<Void> validate(RewardTransaction trx) {
    String status = trx.getStatus();
    if (SyncTrxStatus.INVOICED.name().equalsIgnoreCase(status) || SyncTrxStatus.REWARDED.name().equalsIgnoreCase(status)) {
      return Mono.empty();
    }
    return Mono.error(new ClientExceptionWithBody(HttpStatus.UNPROCESSABLE_ENTITY, TRANSACTION_STATUS_NOT_ALLOWED, "Transaction status not allowed for full reversal"));
  }
}
