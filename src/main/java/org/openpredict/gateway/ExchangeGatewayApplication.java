package org.openpredict.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = {
        "org.openpredict.gateway"
})
@PropertySource("application.properties")
public class ExchangeGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExchangeGatewayApplication.class, args);
    }

}
