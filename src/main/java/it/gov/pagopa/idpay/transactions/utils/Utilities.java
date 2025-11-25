package it.gov.pagopa.idpay.transactions.utils;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;

@Slf4j
public class Utilities {

  private Utilities() {}

  public static String sanitizeString(String str){
    return str.replaceAll("[\\r\\n]", "").replaceAll("[^\\w\\s-]", "");
  }

  public static void checkFileExtensionOrThrow(FilePart file) {
    if (file == null) {
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, ExceptionCode.GENERIC_ERROR, "File is required");
    }

    String filename = file.filename();
    if (!filename.toLowerCase().endsWith(".pdf") && !filename.toLowerCase().endsWith(".xml")) {
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
          ExceptionCode.GENERIC_ERROR, "File must be a PDF or XML");
    }
  }
}
