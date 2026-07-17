package com.apunto.engine.service.copy.calibration;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Data
@Validated
@Component
@ConfigurationProperties(prefix = "copy.calibration")
public class ExecutionCalibrationProperties {

    @Min(1)
    private int mediumSampleSize = 10;

    @Min(1)
    private int highSampleSize = 30;

    @NotNull
    private Duration maximumAge = Duration.ofDays(14);

    @AssertTrue(message = "copy.calibration policy is invalid")
    public boolean isValidPolicy() {
        return highSampleSize >= mediumSampleSize
                && maximumAge != null
                && !maximumAge.isZero()
                && !maximumAge.isNegative();
    }
}
