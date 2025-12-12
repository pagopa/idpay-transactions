package it.gov.pagopa.idpay.transactions.config;

import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.common.web.exception.ServiceException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ServiceExceptionConfig {
    @Bean
    public Map<Class<? extends ServiceException>, HttpStatus> serviceExceptionMapper() {
        Map<Class<? extends ServiceException>, HttpStatus> exceptionMap = new HashMap<>();

        // NOT FOUND
        exceptionMap.put(RewardBatchNotFound.class, HttpStatus.NOT_FOUND);

        // FORBIDDEN
        exceptionMap.put(RoleNotAllowedForL1PromotionException.class, HttpStatus.FORBIDDEN);
        exceptionMap.put(RoleNotAllowedForL2PromotionException.class, HttpStatus.FORBIDDEN);

        // BAD REQUEST
        exceptionMap.put(BatchNotElaborated15PercentException.class, HttpStatus.BAD_REQUEST);
        exceptionMap.put(InvalidBatchStateForPromotionException.class, HttpStatus.BAD_REQUEST);

        return exceptionMap;
    }
}
