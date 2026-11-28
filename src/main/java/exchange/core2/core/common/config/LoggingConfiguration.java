/*
 * Copyright 2019-2026 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
