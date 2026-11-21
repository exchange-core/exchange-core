package exchange.core2.core.common.config;

import java.util.EnumSet;

/**
 * Exchange core logging configuration
 */
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

    public LoggingConfiguration(EnumSet<LoggingLevel> loggingLevels) {
        this.loggingLevels = loggingLevels;
    }

    public static LoggingConfigurationBuilder builder() {
        return new LoggingConfigurationBuilder();
    }

    public EnumSet<LoggingLevel> getLoggingLevels() {
        return this.loggingLevels;
    }

    @Override
    public String toString() {
        return "LoggingConfiguration(loggingLevels=" + this.getLoggingLevels() + ")";
    }

    public static class LoggingConfigurationBuilder {
        private EnumSet<LoggingLevel> loggingLevels;

        LoggingConfigurationBuilder() {
        }

        public LoggingConfigurationBuilder loggingLevels(EnumSet<LoggingLevel> loggingLevels) {
            this.loggingLevels = loggingLevels;
            return this;
        }

        public LoggingConfiguration build() {
            return new LoggingConfiguration(this.loggingLevels);
        }

        @Override
        public String toString() {
            return "LoggingConfiguration.LoggingConfigurationBuilder(loggingLevels=" + this.loggingLevels + ")";
        }
    }
}
