package org.openpredict.exchange.rest;

import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.openpredict.exchange.beans.api.rest.RestApiPlaceOrder;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.ExchangeCore;
import org.rapidoid.setup.On;
import org.rapidoid.u.U;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
@Slf4j
public class RestAsyncTradeApiController {

    @Autowired
    private ExchangeCore exchangeCore;

    // TODO per user
    //private ConcurrentHashMap<Long, Long> userCookies = new ConcurrentHashMap<>();

    private ObjectIntHashMap<String> symbolIds = new ObjectIntHashMap<>();


    @PostConstruct
    public void initRestApi() {


        On.post("/asyncTradeApi/v1/placeNewOrder").json((RestApiPlaceOrder placeOrder) -> {
            log.info(">>> ", placeOrder);
            //exchangeApi.submitCommand(placeOrder);

            RingBuffer<OrderCommand> ringBuffer = exchangeCore.getRingBuffer();

            int symbol = symbolIds.get(placeOrder.getSymbol());

            if (symbol == 0) {
                return U.map("status", "failed", "description", "unknown symbol");
            }

            // TODO chose proper exchange core instance based on symbol

            ringBuffer.publishEvent((cmd, seq) -> {

                cmd.command = OrderCommandType.PLACE_ORDER;
                cmd.resultCode = CommandResultCode.NEW;

                cmd.price = placeOrder.getPrice();
                cmd.size = placeOrder.getSize();
                cmd.orderId = seq;
                cmd.timestamp = System.currentTimeMillis();
                cmd.action = placeOrder.getAction();
                cmd.orderType = placeOrder.getOrderType();
                cmd.symbol = symbol;
                cmd.uid = placeOrder.getUid();

                // TODO fix
                //userCookies.put(seq, placeOrder.getCookieId());
            });


            return U.map("status", "ok");

        });


    }


}
