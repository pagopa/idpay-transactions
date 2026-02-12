package it.gov.pagopa.idpay.transactions.dto;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ReportRequest {

    @NotNull
    private LocalDateTime startPeriod;
    @NotNull
    private LocalDateTime endPeriod;
    @NotNull
    private ReportType reportType;

}

