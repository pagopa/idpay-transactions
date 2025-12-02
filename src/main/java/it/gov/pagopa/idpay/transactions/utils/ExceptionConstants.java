package it.gov.pagopa.idpay.transactions.utils;


public final class ExceptionConstants {
    private ExceptionConstants(){}

    public static final class ExceptionCode {
        private ExceptionCode(){}

        public static final String TOO_MANY_REQUESTS = "TRANSACTIONS_TOO_MANY_REQUESTS";
        public static final String GENERIC_ERROR = "TRANSACTIONS_GENERIC_ERROR";
        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = "TRANSACTIONS_MISSING_MANDATORY_FILTERS";
        public static final String REWARD_BATCH_NOT_FOUND = "REWARD_BATCH_NOT_FOUND";
        public static final String REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE = "REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE";
        public static final String REWARD_BATCH_ALREADY_APPROVED = "REWARD_BATCH_ALREADY_APPROVED";
        public static final String REWARD_BATCH_INVALID_REQUEST = "REWARD_BATCH_INVALID_REQUEST";
        public static final String REWARD_BATCH_MONTH_TOO_EARLY = "REWARD_BATCH_MONTH_TOO_EARLY";
        public static final String REWARD_BATCH_TOO_MANY_REQUESTS = "REWARD_BATCH_TOO_MANY_REQUESTS";
        public static final String REWARD_BATCH_GENERIC_ERROR = "REWARD_BATCH_GENERIC_ERROR";
        public static final String POINT_OF_SALE_NOT_ALLOWED = "POINT_OF_SALE_NOT_ALLOWED";
        public static final String REASON_FIELD_IS_MANDATORY = "REASON_FIELD_IS_MANDATORY";

    }

    public static final class ExceptionMessage {
        private ExceptionMessage(){}

        public static final String TOO_MANY_REQUESTS = "Too Many Requests";
        public static final String GENERIC_ERROR = "Something gone wrong";
        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = "Mandatory filters are missing. Insert one of the following options: 1) idTrxIssuer 2) userId, trxDateStart and trxDateEnd";
        public static final String TRANSACTION_MISSING_INVOICE = "Invoice missing from transaction for which download was required";
        public static final String TRANSACTION_NOT_FOUND = "Transaction not found for ID: %s";
        public static final String ERROR_ON_GET_FILE_URL_REQUEST = "Error occurred while attempting to get file url";
        public static final String MISSING_TRANSACTIONS_FILTERS = "Mandatory filters are missing. Insert one of the following options: 1) organizationRole 2) merchantId";
        public static final String REWARD_BATCH_STATUS_MISMATCH = "Operation not allowed: the batch is no longer in CREATED status";

        public static final String REASON_FIELD_IS_MANDATORY = "Reason field is mandatory";
        public static final String ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH = "Reward batch  %s not  found  or  not in  a  valid  state";
        public static final String ERROR_MESSAGE_NOT_FOUND_BATCH = "Reward batch  %s not  found";
        public static final String ERROR_MESSAGE_INVALID_STATE_BATCH = "Reward batch  %s  not in  a  valid  state";
    }
}
