package exchange.core2.tests;

import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.steps.OrderStepdefs;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
    @ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "pretty, html:target/cucumber/cucumber.html"),
})
@Slf4j
public class RunCukesDirectLatencyTests {

    @BeforeClass
    public static void beforeClass() {
        OrderStepdefs.testPerformanceConfiguration = PerformanceConfiguration.latencyPerformanceBuilder().build();
    }

    @AfterClass
    public static void afterClass() {
        OrderStepdefs.testPerformanceConfiguration = null;
    }

}
