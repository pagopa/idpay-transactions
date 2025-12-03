package it.gov.pagopa.idpay.transactions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication(scanBasePackages = "it.gov.pagopa")
@EnableCaching
public class IdpayTransactionsApplication {

	public static void main(String[] args) {
		SpringApplication.run(IdpayTransactionsApplication.class, args);
	}

}
