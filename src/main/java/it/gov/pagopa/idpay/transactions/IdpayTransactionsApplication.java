package it.gov.pagopa.idpay.transactions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "it.gov.pagopa")
public class IdpayTransactionsApplication {

	public static void main(String[] args) {
		SpringApplication.run(IdpayTransactionsApplication.class, args);
	}

}
