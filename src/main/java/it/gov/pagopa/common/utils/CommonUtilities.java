package it.gov.pagopa.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import org.springframework.messaging.Message;

import java.util.function.Consumer;


public final class CommonUtilities {
    private CommonUtilities(){}

    /** It will try to deserialize a message, eventually notifying the error  */
    public static <T> T deserializeMessage(Message<?> message, ObjectReader objectReader, Consumer<Throwable> onError) {
        try {
            String payload = readMessagePayload(message);
            return objectReader.readValue(payload);
        } catch (JsonProcessingException e) {
            onError.accept(e);
            return null;
        }
    }

    /** It will read message payload checking if it's a byte[] or String */
    public static String readMessagePayload(Message<?> message) {
        String payload;
        if(message.getPayload() instanceof byte[] bytes){
            payload=new String(bytes);
        } else {
            payload= message.getPayload().toString();
        }
        return payload;
    }

    /** To read Message header value */
    public static Object getHeaderValue(Message<?> message, String headerName) {
        return  message.getHeaders().get(headerName);
    }

}
