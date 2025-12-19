package it.gov.pagopa.idpay.transactions.notifier;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionKafkaDTO;
import org.springframework.messaging.Message;

public interface TransactionNotifierService {

  boolean notify(RewardTransactionKafkaDTO trx, String key);
  Message<RewardTransactionKafkaDTO> buildMessage(RewardTransactionKafkaDTO trx, String key);

}
