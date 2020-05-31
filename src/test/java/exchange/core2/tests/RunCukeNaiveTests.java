package exchange.core2.tests;

import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.steps.OrderStepdefs;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(plugin = {"pretty", "html:target/cucumber"}, strict = true)
@Slf4j
public class RunCukeNaiveTests {

    @BeforeClass
    public static void beforeClass() {
        OrderStepdefs.testPerformanceConfiguration = PerformanceConfiguration.baseBuilder().build();
    }

    @AfterClass
    public static void afterClass() {
        OrderStepdefs.testPerformanceConfiguration = null;
    }

}
