package org.openpredict.exchange.rdma;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@Configuration
@Slf4j
public class Config {

    @Bean
    public Consumer<OrderCommand> resultsConsumer() {
        return c -> {
            //  log.debug("result: {}", c);
        };
    }

    @Bean
    public List<Integer> orderBooks(){
        return Collections.singletonList(10);
    }

}
