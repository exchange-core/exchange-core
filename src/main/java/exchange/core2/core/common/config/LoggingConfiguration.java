package exchange.core2.core.common.config;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.EnumSet;


/**
 * Exchange core logging configuration
 */
@AllArgsConstructor
@Getter
@Builder
@ToString
public final class LoggingConfiguration {

    // only warnings
    public static LoggingConfiguration DEFAULT = LoggingConfiguration.builder()
            .loggingLevels(EnumSet.of(LoggingLevel.LOGGING_WARNINGS))
            .build();

    private final EnumSet<LoggingLevel> loggingLevels;

    public enum LoggingLevel {
        LOGGING_WARNINGS,
        LOGGING_RISK_DEBUG,
        LOGGING_MATCHING_DEBUG
    }
}
