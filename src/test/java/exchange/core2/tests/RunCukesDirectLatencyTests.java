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
package exchange.core2.tests;

import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.steps.OrderStepdefs;
import io.cucumber.plugin.EventListener;
import io.cucumber.plugin.event.EventPublisher;
import io.cucumber.plugin.event.TestRunFinished;
import io.cucumber.plugin.event.TestRunStarted;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.ConfigurationParameters;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.SelectClasspathResources;
import org.junit.platform.suite.api.Suite;

@Suite
@IncludeEngines("cucumber")
@SelectClasspathResources({
    @SelectClasspathResource("exchange/core2/tests/features/basic.feature"),
    @SelectClasspathResource("exchange/core2/tests/features/risk.feature")
})
@ConfigurationParameters({
    @ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "pretty, html:target/cucumber/direct-latency.html, exchange.core2.tests.RunCukesDirectLatencyTests$CukeNaiveLifeCycleHandler"),
})
public class RunCukesDirectLatencyTests {

    public static class CukeNaiveLifeCycleHandler implements EventListener {

        @Override
        public void setEventPublisher(EventPublisher eventPublisher) {
            eventPublisher.registerHandlerFor(TestRunStarted.class,
                event -> OrderStepdefs.testPerformanceConfiguration = PerformanceConfiguration.latencyPerformanceBuilder()
                    .threadFactory(Thread::new)
                    .build());
            eventPublisher.registerHandlerFor(TestRunFinished.class,
                event -> OrderStepdefs.testPerformanceConfiguration = null);
        }
    }
}
