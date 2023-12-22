package it.gov.pagopa.idpay.transactions.utils;

public final class ExceptionConstants {
    private ExceptionConstants(){}

    public static final class ExceptionCode {
        private ExceptionCode(){}

        public static final String TOO_MANY_REQUESTS = "TRANSACTIONS_TOO_MANY_REQUESTS";
        public static final String GENERIC_ERROR = "TRANSACTIONS_GENERIC_ERROR";
        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = "TRANSACTIONS_MISSING_MANDATORY_FILTERS";
    }

    public static final class ExceptionMessage {
        private ExceptionMessage(){}

        public static final String TOO_MANY_REQUESTS = "Too Many Requests";
        public static final String GENERIC_ERROR = "Something gone wrong";
        public static final String TRANSACTIONS_MISSING_MANDATORY_FILTERS = "Mandatory filters are missing. Insert one of the following options: 1) idTrxIssuer 2) userId, trxDateStart and trxDateEnd";
    }
}
