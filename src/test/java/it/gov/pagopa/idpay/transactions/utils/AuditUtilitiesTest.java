package it.gov.pagopa.idpay.transactions.utils;

import ch.qos.logback.classic.LoggerContext;
import it.gov.pagopa.common.utils.AuditLogger;
import it.gov.pagopa.common.utils.MemoryAppender;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class AuditUtilitiesTest {
    private static final String INITIATIVE_ID = "TEST_INITIATIVE_ID";
    private final AuditUtilities auditUtilities = new AuditUtilities();
    private MemoryAppender memoryAppender;

    @BeforeEach
    public void setup() {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("AUDIT");
        memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.setLevel(ch.qos.logback.classic.Level.INFO);
        logger.addAppender(memoryAppender);
        memoryAppender.start();
    }

    @Test
    void logTransactionsDeleted() {
        auditUtilities.logTransactionsDeleted(5L, INITIATIVE_ID);

        Assertions.assertEquals(
                ("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=Transactions dstip=%s msg=Deleted transactions" +
                        " cs1Label=initiativeId cs1=%s cs2Label=numberTransactions cs2=%s")
                        .formatted(
                                AuditLogger.SRCIP,
                                INITIATIVE_ID,
                                5L
                        ),
                memoryAppender.getLoggedEvents().get(0).getFormattedMessage()
        );
    }

    @Test
    void logTransactionsStatusChanged_noChecks() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(false);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(false);
        dto.setPriceError(false);
        dto.setBonusError(false);
        dto.setSellerReferenceError(false);
        dto.setAccountingDocumentError(false);

        auditUtilities.logTransactionsStatusChanged("SUSPENDED", INITIATIVE_ID, "trx1,trx2", dto);

        String expectedLog = ("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=Transactions dstip=%s msg=suspended transactions" +
                " cs1Label=initiativeId cs1=%s cs2Label=status cs2=%s cs3Label=transactionIds cs3=%s cs4Label=trueChecksError cs4=%s")
                .formatted(AuditLogger.SRCIP, INITIATIVE_ID, "SUSPENDED", "trx1,trx2", "");

        Assertions.assertEquals(expectedLog, memoryAppender.getLoggedEvents().getFirst().getFormattedMessage());
    }

    @Test
    void logTransactionsStatusChanged_withSomeChecks() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(true);
        dto.setPriceError(false);
        dto.setBonusError(true);
        dto.setSellerReferenceError(false);
        dto.setAccountingDocumentError(false);

        auditUtilities.logTransactionsStatusChanged("SUSPENDED", INITIATIVE_ID, "trx42", dto);

        String expectedLog = ("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=Transactions dstip=%s msg=suspended transactions" +
                " cs1Label=initiativeId cs1=%s cs2Label=status cs2=%s cs3Label=transactionIds cs3=%s cs4Label=trueChecksError cs4=%s")
                .formatted(AuditLogger.SRCIP, INITIATIVE_ID, "SUSPENDED", "trx42", "cfError,disposalRaeeError,bonus");

        Assertions.assertEquals(expectedLog, memoryAppender.getLoggedEvents().getFirst().getFormattedMessage());
    }

    @Test
    void logTransactionsStatusChanged_withAllChecks() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        dto.setProductEligibilityError(true);
        dto.setDisposalRaeeError(true);
        dto.setPriceError(true);
        dto.setBonusError(true);
        dto.setSellerReferenceError(true);
        dto.setAccountingDocumentError(true);

        auditUtilities.logTransactionsStatusChanged("SUSPENDED", INITIATIVE_ID, "trx42", dto);

        String expectedLog = ("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=Transactions dstip=%s msg=suspended transactions" +
                " cs1Label=initiativeId cs1=%s cs2Label=status cs2=%s cs3Label=transactionIds cs3=%s cs4Label=trueChecksError cs4=%s")
                .formatted(AuditLogger.SRCIP, INITIATIVE_ID, "SUSPENDED", "trx42", "cfError,productEligibilityError,disposalRaeeError,price,bonus,sellerReference,accountingDocument");

        Assertions.assertEquals(expectedLog, memoryAppender.getLoggedEvents().getFirst().getFormattedMessage());
    }

    @Test
    void logTransactionsStatusChanged_nullChecks() {
        ChecksErrorDTO dto = null;

        auditUtilities.logTransactionsStatusChanged("APPROVED", INITIATIVE_ID, "trx100,trx101", dto);

        String expectedLog = ("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=Transactions dstip=%s msg=approved transactions" +
                " cs1Label=initiativeId cs1=%s cs2Label=status cs2=%s cs3Label=transactionIds cs3=%s cs4Label=trueChecksError cs4=%s")
                .formatted(AuditLogger.SRCIP, INITIATIVE_ID, "APPROVED", "trx100,trx101", "");

        Assertions.assertEquals(expectedLog, memoryAppender.getLoggedEvents().getFirst().getFormattedMessage());
    }

}
