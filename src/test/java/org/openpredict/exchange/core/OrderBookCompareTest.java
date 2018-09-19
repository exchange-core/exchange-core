package org.openpredict.exchange.core;

import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class OrderBookCompareTest {

    @Test
    public void multipleCommandsTest() {

        TestOrdersGenerator generator = new TestOrdersGenerator();

        int tranNum = 10_000;

        IOrderBook orderBook = new OrderBookFast(4096);
        IOrderBook orderBookRef = new OrderBookSlow();


        TestOrdersGenerator.GenResult genResult = generator.generateCommands(tranNum, 200, Longs.asList(10, 11, 12, 13, 14, 15));

        int i = 0;
        for (OrderCommand cmd : genResult.getCommands()) {

            cmd.orderId += 100;

            log.debug("{}. {}", ++i, cmd);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            orderBook.processCommand(cmd);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            orderBookRef.processCommand(cmd);


            assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));


//            assertEquals(orderBook.hashCode(), orderBookRef.hashCode());
            assertEquals(orderBook, orderBookRef);

            // TODO compare events!
        }

    }

}
