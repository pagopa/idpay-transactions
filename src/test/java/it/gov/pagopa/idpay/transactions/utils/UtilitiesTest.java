package it.gov.pagopa.idpay.transactions.utils;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UtilitiesTest {

    @Test
    void checkFileExtensionOrThrowShouldRejectMissingFile() {
        ClientExceptionWithBody exception = assertThrows(
                ClientExceptionWithBody.class,
                () -> Utilities.checkFileExtensionOrThrow(null)
        );

        assertEquals(HttpStatus.BAD_REQUEST, exception.getHttpStatus());
        assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, exception.getCode());
        assertEquals("File is required", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"report.PDF", "user-details.Xml"})
    void checkFileExtensionOrThrowShouldAcceptPdfAndXmlCaseInsensitively(String filename) {
        FilePart file = mock(FilePart.class);
        when(file.filename()).thenReturn(filename);

        assertDoesNotThrow(() -> Utilities.checkFileExtensionOrThrow(file));
    }

    @Test
    void checkFileExtensionOrThrowShouldRejectUnsupportedExtension() {
        FilePart file = mock(FilePart.class);
        when(file.filename()).thenReturn("report.csv");

        ClientExceptionWithBody exception = assertThrows(
                ClientExceptionWithBody.class,
                () -> Utilities.checkFileExtensionOrThrow(file)
        );

        assertEquals(HttpStatus.BAD_REQUEST, exception.getHttpStatus());
        assertEquals(ExceptionConstants.ExceptionCode.GENERIC_ERROR, exception.getCode());
        assertEquals("File must be a PDF or XML", exception.getMessage());
    }
}
