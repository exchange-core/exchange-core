package org.openpredict.exchange;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = {
        "org.openpredict.exchange"
})
@PropertySource("application.properties")
@Configuration
public class ExchangeCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExchangeCoreApplication.class, args);
    }

//    @Bean
//    public Consumer<OrderCommand> resultsConsumer() {
//        return cmd -> {
//            System.out.println(">>>" + cmd);
//        };
//    }

}
