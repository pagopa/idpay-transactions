package it.gov.pagopa.idpay.transactions.utils;

import it.gov.pagopa.common.utils.AuditLogger;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j(topic = "AUDIT")
public class AuditUtilities {

    private static final String CEF = String.format("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=RewardNotification dstip=%s", AuditLogger.SRCIP);
    private static final String CEF_BASE_PATTERN = CEF + " msg={}";
    private static final String CEF_PATTERN_DELETE = CEF_BASE_PATTERN + " sdeletedTransactions={} cs1Label=initiativeId cs1={}";

    //region deleted
    public void logTransactionsDeleted(Long deletedTransactions, String initiativeId) {
        AuditLogger.logAuditString(
                CEF_PATTERN_DELETE,
                "Transactions deleted", deletedTransactions.toString(), initiativeId
        );
    }
    //endregion
}