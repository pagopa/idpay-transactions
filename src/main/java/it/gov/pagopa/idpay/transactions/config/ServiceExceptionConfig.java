package it.gov.pagopa.idpay.transactions.config;

import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
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

        // BadRequest
        exceptionMap.put(RewardBatchNotFound.class, HttpStatus.BAD_REQUEST);

        return exceptionMap;
    }
}
