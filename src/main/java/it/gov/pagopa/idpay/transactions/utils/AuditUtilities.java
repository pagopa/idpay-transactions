package it.gov.pagopa.idpay.transactions.utils;

import it.gov.pagopa.common.utils.AuditLogger;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Component
@AllArgsConstructor
@Slf4j(topic = "AUDIT")
public class AuditUtilities {

    private static final String CEF = String.format("CEF:0|PagoPa|IDPAY|1.0|7|User interaction|2| event=Transactions dstip=%s", AuditLogger.SRCIP);
    private static final String CEF_BASE_PATTERN = CEF + " msg={}";
    private static final String CEF_PATTERN_DELETE = CEF_BASE_PATTERN + " cs1Label=initiativeId cs1={} cs2Label=numberTransactions cs2={}";
    private static final String CEF_PATTERN_STATUS_CHANGE = CEF_BASE_PATTERN + " cs1Label=initiativeId cs1={} cs2Label=status cs2={} cs3Label=transactionIds cs3={} cs4Label=trueChecksError cs4={}";


    //region deleted
    public void logTransactionsDeleted(Long deletedTransactions, String initiativeId) {
        AuditLogger.logAuditString(
                CEF_PATTERN_DELETE,
                "Deleted transactions", initiativeId, deletedTransactions.toString()
        );
    }
    //endregion

    //region suspended
    public void logTransactionsStatusChanged(String status, String initiativeId, String transactionIds, ChecksErrorDTO trueChecksError) {
        String checks = String.join(",", extractTrueChecks(trueChecksError));

        AuditLogger.logAuditString(
                CEF_PATTERN_STATUS_CHANGE, status.toLowerCase(Locale.ROOT) + " transactions", initiativeId, status, transactionIds, checks
        );
    }
    //endregion

    //region utils
    private List<String> extractTrueChecks(ChecksErrorDTO dto) {
        if (dto == null) {
            return List.of();
        }

        List<String> activeChecks = new ArrayList<>();

        if (dto.isCfError()) activeChecks.add("cfError");
        if (dto.isProductEligibilityError()) activeChecks.add("productEligibilityError");
        if (dto.isDisposalRaeeError()) activeChecks.add("disposalRaeeError");
        if (dto.isPriceError()) activeChecks.add("price");
        if (dto.isBonusError()) activeChecks.add("bonus");
        if (dto.isSellerReferenceError()) activeChecks.add("sellerReference");
        if (dto.isAccountingDocumentError()) activeChecks.add("accountingDocument");

        return activeChecks;
    }
    //endregion
}