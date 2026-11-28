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

import exchange.core2.core.processors.journaling.DiskSerializationProcessor;
import exchange.core2.core.processors.journaling.DiskSerializationProcessorConfiguration;
import exchange.core2.core.processors.journaling.DummySerializationProcessor;
import exchange.core2.core.processors.journaling.ISerializationProcessor;

import java.util.function.Function;

public class SerializationConfiguration {

    // no serialization
    public static final SerializationConfiguration DEFAULT = SerializationConfiguration.builder()
            .enableJournaling(false)
            .serializationProcessorFactory(cfg -> DummySerializationProcessor.INSTANCE)
            .build();

    // no journaling, only snapshots
    public static final SerializationConfiguration DISK_SNAPSHOT_ONLY = SerializationConfiguration.builder()
            .enableJournaling(false)
            .serializationProcessorFactory(exchangeCfg -> new DiskSerializationProcessor(exchangeCfg, DiskSerializationProcessorConfiguration.createDefaultConfig()))
            .build();

    // snapshots and journaling
    public static final SerializationConfiguration DISK_JOURNALING = SerializationConfiguration.builder()
            .enableJournaling(true)
            .serializationProcessorFactory(exchangeCfg -> new DiskSerializationProcessor(exchangeCfg, DiskSerializationProcessorConfiguration.createDefaultConfig()))
            .build();

    /*
     * Enables journaling.
     * Set to false for analytics instances.
     */
    private final boolean enableJournaling;

    /*
     * Serialization processor implementations
     */
    private final Function<ExchangeConfiguration, ? extends ISerializationProcessor> serializationProcessorFactory;



    public SerializationConfiguration(
            boolean enableJournaling,
            Function<ExchangeConfiguration, ? extends ISerializationProcessor> serializationProcessorFactory) {
        this.enableJournaling = enableJournaling;
        this.serializationProcessorFactory = serializationProcessorFactory;
    }

    public static SerializationConfigurationBuilder builder() {
        return new SerializationConfigurationBuilder();
    }

    public boolean isEnableJournaling() {
        return this.enableJournaling;
    }

    public Function<ExchangeConfiguration, ? extends ISerializationProcessor> getSerializationProcessorFactory() {
        return this.serializationProcessorFactory;
    }

    @Override
    public String toString() {
        return "SerializationConfiguration(enableJournaling=" + this.isEnableJournaling()
                + ", serializationProcessorFactory=" + this.getSerializationProcessorFactory() + ")";
    }

    public static class SerializationConfigurationBuilder {
        private boolean enableJournaling;
        private Function<ExchangeConfiguration, ? extends ISerializationProcessor> serializationProcessorFactory;

        SerializationConfigurationBuilder() {
        }

        public SerializationConfigurationBuilder enableJournaling(boolean enableJournaling) {
            this.enableJournaling = enableJournaling;
            return this;
        }

        public SerializationConfigurationBuilder serializationProcessorFactory(Function<ExchangeConfiguration, ? extends ISerializationProcessor> serializationProcessorFactory) {
            this.serializationProcessorFactory = serializationProcessorFactory;
            return this;
        }

        public SerializationConfiguration build() {
            return new SerializationConfiguration(this.enableJournaling, this.serializationProcessorFactory);
        }

        @Override
        public String toString() {
            return "SerializationConfiguration.SerializationConfigurationBuilder(enableJournaling=" + this.enableJournaling
                    + ", serializationProcessorFactory=" + this.serializationProcessorFactory + ")";
        }
    }
}
