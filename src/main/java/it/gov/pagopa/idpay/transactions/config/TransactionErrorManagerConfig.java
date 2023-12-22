package it.gov.pagopa.idpay.transactions.config;

import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransactionErrorManagerConfig {

  @Bean
  ErrorDTO defaultErrorDTO() {
    return new ErrorDTO(
        ExceptionConstants.ExceptionCode.GENERIC_ERROR,
            ExceptionConstants.ExceptionMessage.GENERIC_ERROR
    );
  }

  @Bean
  ErrorDTO tooManyRequestsErrorDTO() {
    return new ErrorDTO(ExceptionConstants.ExceptionCode.TOO_MANY_REQUESTS, ExceptionConstants.ExceptionMessage.TOO_MANY_REQUESTS);
  }
}
