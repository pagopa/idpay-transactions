package it.gov.pagopa.idpay.transactions.config;

import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.*;
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TransactionErrorManagerConfig.class)
class TransactionErrorManagerConfigTest {

    @Autowired
    private TransactionErrorManagerConfig transactionErrorManagerConfig;
    @Test
    void defaultErrorDTO() {
        ErrorDTO errorDTO = transactionErrorManagerConfig.defaultErrorDTO();

        assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, errorDTO.getCode());
        assertEquals(ExceptionConstants.ExceptionMessage.GENERIC_ERROR, errorDTO.getMessage());
    }

    @Test
    void tooManyRequestsErrorDTO() {
        ErrorDTO errorDTO = transactionErrorManagerConfig.tooManyRequestsErrorDTO();

        assertEquals(ExceptionConstants.ExceptionCode.TOO_MANY_REQUESTS, errorDTO.getCode());
        assertEquals(ExceptionConstants.ExceptionMessage.TOO_MANY_REQUESTS, errorDTO.getMessage());

    }
}