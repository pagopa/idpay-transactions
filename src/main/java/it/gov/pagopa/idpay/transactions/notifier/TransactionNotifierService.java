package it.gov.pagopa.idpay.transactions.notifier;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.messaging.Message;

public interface TransactionNotifierService {

  boolean notify(RewardTransaction trx, String key);
  Message<RewardTransaction> buildMessage(RewardTransaction trx, String key);

}
