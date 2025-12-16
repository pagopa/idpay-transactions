package it.gov.pagopa.idpay.transactions.utils;


public final class ExceptionConstants {
    private ExceptionConstants(){}

    public static final class ExceptionCode {
        private ExceptionCode(){}

        public static final String TOO_MANY_REQUESTS = "TRANSACTIONS_TOO_MANY_REQUESTSpackage it.gov.pagopa.idpay.transactions.utils;\n" +
                "\n" +
                "\n" +
                "public final class ExceptionConstants {\n" +
                "    private ExceptionConstants(){}\n" +
                "\n" +
                "    public static final class ExceptionCode {\n" +
                "        private ExceptionCode(){}\n" +
                "\n" +
                "        public static final String TOO_MANY_REQUESTS = \"TRANSACTIONS_TOO_MANY_REQUESTS\";\n" +
                "        public static final String GENERIC_ERROR = \"TRANSACTIONS_GENERIC_ERROR\";\n" +
                "        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = \"TRANSACTIONS_MISSING_MANDATORY_FILTERS\";\n" +
                "        public static final String REWARD_BATCH_NOT_FOUND = \"REWARD_BATCH_NOT_FOUND\";\n" +
                "        public static final String REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE = \"REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE\";\n" +
                "        public static final String REWARD_BATCH_ALREADY_APPROVED = \"REWARD_BATCH_ALREADY_APPROVED\";\n" +
                "        public static final String REWARD_BATCH_INVALID_REQUEST = \"REWARD_BATCH_INVALID_REQUEST\";\n" +
                "        public static final String REWARD_BATCH_MONTH_TOO_EARLY = \"REWARD_BATCH_MONTH_TOO_EARLY\";\n" +
                "        public static final String REWARD_BATCH_TOO_MANY_REQUESTS = \"REWARD_BATCH_TOO_MANY_REQUESTS\";\n" +
                "        public static final String REWARD_BATCH_GENERIC_ERROR = \"REWARD_BATCH_GENERIC_ERROR\";\n" +
                "        public static final String POINT_OF_SALE_NOT_ALLOWED = \"POINT_OF_SALE_NOT_ALLOWED\";\n" +
                "        public static final String REASON_FIELD_IS_MANDATORY = \"REASON_FIELD_IS_MANDATORY\";\n" +
                "\n" +
                "        public static final String ROLE_NOT_ALLOWED_FOR_L1_PROMOTION = \"ROLE_NOT_ALLOWED_FOR_L1_PROMOTION\";\n" +
                "        public static final String ROLE_NOT_ALLOWED_FOR_L2_PROMOTION = \"ROLE_NOT_ALLOWED_FOR_L2_PROMOTION\";\n" +
                "        public static final String BATCH_NOT_ELABORATED_15_PERCENT = \"BATCH_NOT_ELABORATED_15_PERCENT\";\n" +
                "        public static final String INVALID_BATCH_STATE_FOR_PROMOTION = \"INVALID_BATCH_STATE_FOR_PROMOTION\";\n" +
                "        public static final String REWARD_BATCH_ALREADY_SENT = \"REWARD_BATCH_ALREADY_SENT\";\n" +
                "    }\n" +
                "\n" +
                "    public static final class ExceptionMessage {\n" +
                "        private ExceptionMessage(){}\n" +
                "\n" +
                "        public static final String TOO_MANY_REQUESTS = \"Too Many Requests\";\n" +
                "        public static final String GENERIC_ERROR = \"Something gone wrong\";\n" +
                "        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = \"Mandatory filters are missing. Insert one of the following options: 1) idTrxIssuer 2) userId, trxDateStart and trxDateEnd\";\n" +
                "        public static final String TRANSACTION_MISSING_INVOICE = \"Invoice missing from transaction for which download was required\";\n" +
                "        public static final String TRANSACTION_NOT_FOUND = \"Transaction not found for ID: %s\";\n" +
                "        public static final String TRANSACTION_NOT_STATUS_INVOICED = \"Transaction is not in invoiced status\";\n" +
                "        public static final String ERROR_ON_GET_FILE_URL_REQUEST = \"Error occurred while attempting to get file url\";\n" +
                "        public static final String MISSING_TRANSACTIONS_FILTERS = \"Mandatory filters are missing. Insert one of the following options: 1) organizationRole 2) merchantId\";\n" +
                "        public static final String REWARD_BATCH_STATUS_MISMATCH = \"Operation not allowed: the batch is no longer in CREATED status\";\n" +
                "\n" +
                "        public static final String REASON_FIELD_IS_MANDATORY = \"Reason field is mandatory\";\n" +
                "        public static final String ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH = \"Reward batch  %s not  found  or  not in  a  valid  state\";\n" +
                "        public static final String ERROR_MESSAGE_NOT_FOUND_BATCH = \"Reward batch  %s not  found\";\n" +
                "        public static final String ERROR_MESSAGE_INVALID_STATE_BATCH = \"Reward batch  %s  not in  a  valid  state\";\n" +
                "\n" +
                "        public static final String ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE = \"Is not possible to approve batch %s because there are previous Batch to approve\";\n" +
                "        public static final String ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT = \"No reward batches found with status SENT\";\n" +
                "        public static final String ERROR_MESSAGE_REWARD_BATCH_ALREADY_SENT = \"Reward batch has already been sent\";\n" +
                "    }\n" +
                "}\n";
        public static final String GENERIC_ERROR = "TRANSACTIONS_GENERIC_ERROR";
        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = "TRANSACTIONS_MISSING_MANDATORY_FILTERS";
        public static final String REWARD_BATCH_NOT_FOUND = "REWARD_BATCH_NOT_FOUND";
        public static final String REWARD_BATCH_NOT_APPROVED = "REWARD_BATCH_NOT_APPROVED";
        public static final String REWARD_BATCH_MISSING_FILENAME = "REWARD_BATCH_MISSING_FILENAME";
        public static final String REWARD_BATCH_INVALID_MERCHANT = "REWARD_BATCH_INVALID_MERCHANT";
        public static final String ROLE_NOT_ALLOWED = "ROLE_NOT_ALLOWED";

        public static final String REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE = "REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE";
        public static final String REWARD_BATCH_ALREADY_APPROVED = "REWARD_BATCH_ALREADY_APPROVED";
        public static final String REWARD_BATCH_INVALID_REQUEST = "REWARD_BATCH_INVALID_REQUEST";
        public static final String REWARD_BATCH_MONTH_TOO_EARLY = "REWARD_BATCH_MONTH_TOO_EARLY";
        public static final String REWARD_BATCH_TOO_MANY_REQUESTS = "REWARD_BATCH_TOO_MANY_REQUESTS";
        public static final String REWARD_BATCH_GENERIC_ERROR = "REWARD_BATCH_GENERIC_ERROR";
        public static final String POINT_OF_SALE_NOT_ALLOWED = "POINT_OF_SALE_NOT_ALLOWED";
        public static final String REASON_FIELD_IS_MANDATORY = "REASON_FIELD_IS_MANDATORY";

        public static final String ROLE_NOT_ALLOWED_FOR_L1_PROMOTION = "ROLE_NOT_ALLOWED_FOR_L1_PROMOTION";
        public static final String ROLE_NOT_ALLOWED_FOR_L2_PROMOTION = "ROLE_NOT_ALLOWED_FOR_L2_PROMOTION";
        public static final String BATCH_NOT_ELABORATED_15_PERCENT = "BATCH_NOT_ELABORATED_15_PERCENT";
        public static final String INVALID_BATCH_STATE_FOR_PROMOTION = "INVALID_BATCH_STATE_FOR_PROMOTION";
        public static final String REWARD_BATCH_ALREADY_SENT = "REWARD_BATCH_ALREADY_SENT";
    }

    public static final class ExceptionMessage {
        private ExceptionMessage(){}

        public static final String TOO_MANY_REQUESTS = "Too Many Requests";
        public static final String GENERIC_ERROR = "Something gone wrong";
        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = "Mandatory filters are missing. Insert one of the following options: 1) idTrxIssuer 2) userId, trxDateStart and trxDateEnd";
        public static final String TRANSACTION_MISSING_INVOICE = "Invoice missing from transaction for which download was required";
        public static final String TRANSACTION_NOT_FOUND = "Transaction not found for ID: %s";
        public static final String TRANSACTION_NOT_STATUS_INVOICED = "Transaction is not in invoiced status";
        public static final String ERROR_ON_GET_FILE_URL_REQUEST = "Error occurred while attempting to get file url";
        public static final String MISSING_TRANSACTIONS_FILTERS = "Mandatory filters are missing. Insert one of the following options: 1) organizationRole 2) merchantId";
        public static final String REWARD_BATCH_STATUS_MISMATCH = "Operation not allowed: the batch is no longer in CREATED status";

        public static final String REASON_FIELD_IS_MANDATORY = "Reason field is mandatory";
        public static final String ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH = "Reward batch  %s not  found  or  not in  a  valid  state";
        public static final String ERROR_MESSAGE_NOT_FOUND_BATCH = "Reward batch  %s not  found";
        public static final String ERROR_MESSAGE_INVALID_STATE_BATCH = "Reward batch  %s  not in  a  valid  state";
        public static final String ERROR_MESSAGE_ROLE_NOT_ALLOWED = "Role not allowed";
        public static final String ERROR_MESSAGE_REWARD_BATCH_NOT_APPROVED = "Reward batch  %s not APPROVED";
        public static final String ERROR_MESSAGE_REWARD_BATCH_MISSING_FILENAME = "Reward batch  %s missing file name";
        public static final String MERCHANT_OR_OPERATOR_HEADER_MANDATORY = "Merchant or operator information is missing";

        public static final String ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE = "Is not possible to approve batch %s because there are previous Batch to approve";
        public static final String ERROR_MESSAGE_NOT_FOUND_REWARD_BATCH_SENT = "No reward batches found with status SENT";
        public static final String ERROR_MESSAGE_REWARD_BATCH_ALREADY_SENT = "Reward batch has already been sent";
        public static final String ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L1_PROMOTION = "Operator not allowed to promote from L1";
        public static final String ERROR_MESSAGE_BATCH_NOT_ELABORATED_15_PERCENT = "At least 15% of transactions must be elaborated";
        public static final String ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L2_PROMOTION = "Operator not allowed to promote from L2";
        public static final String ERROR_MESSAGE_INVALID_BATCH_STATE_FOR_PROMOTION = "Invalid state for batch promotion";
    }
}
