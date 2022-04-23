package exchange.core2.tests;

import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.steps.OrderStepdefs;
import io.cucumber.plugin.EventListener;
import io.cucumber.plugin.event.EventPublisher;
import io.cucumber.plugin.event.TestRunFinished;
import io.cucumber.plugin.event.TestRunStarted;
import lombok.extern.slf4j.Slf4j;
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
    @ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "pretty, html:target/cucumber/cucumber.html, exchange.core2.tests.RunCukeNaiveTests$CukeNaiveLifeCycleHandler"),
})
@Slf4j
public class RunCukeNaiveTests {

    public static class CukeNaiveLifeCycleHandler implements EventListener {

        @Override
        public void setEventPublisher(EventPublisher eventPublisher) {
            eventPublisher.registerHandlerFor(TestRunStarted.class,
                event -> OrderStepdefs.testPerformanceConfiguration = PerformanceConfiguration.baseBuilder().build());
            eventPublisher.registerHandlerFor(TestRunFinished.class,
                event -> OrderStepdefs.testPerformanceConfiguration = null);
        }
    }
}
