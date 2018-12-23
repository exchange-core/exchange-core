package org.openpredict.exchange.rest;

import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
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
import java.math.BigDecimal;
import java.util.Optional;

@Service
@Slf4j
public class RestAsyncTradeApiController {

    @Autowired
    private ExchangeCore exchangeCore;

    @Autowired
    private GatewayState gatewayState;

    // TODO per user
    //private ConcurrentHashMap<Long, Long> userCookies = new ConcurrentHashMap<>();

    @PostConstruct
    public void initRestApi() {


        On.post("/asyncTradeApi/v1/placeNewOrder").json((RestApiPlaceOrder placeOrder) -> {
            log.info(">>> ", placeOrder);
            //exchangeApi.submitCommand(placeOrder);

            final RingBuffer<OrderCommand> ringBuffer = exchangeCore.getRingBuffer();

            final Optional<GatewaySymbolSpecification> specOpt = gatewayState.getSymbolId(placeOrder.getSymbol());

            if (!specOpt.isPresent()) {
                return U.map("status", "failed", "description", "unknown symbol");
            }

            final GatewaySymbolSpecification specification = specOpt.get();

            final BigDecimal price = new BigDecimal(placeOrder.getPrice());
            final long longPrice = price.longValue();

            final BigDecimal size = new BigDecimal(placeOrder.getSize());
            final long longSize = size.longValue();


            // TODO chose proper exchange core instance based on symbol
            ringBuffer.publishEvent((cmd, seq) -> {

                cmd.command = OrderCommandType.PLACE_ORDER;
                cmd.resultCode = CommandResultCode.NEW;

                cmd.price = longPrice;
                cmd.size = longSize;
                cmd.orderId = seq;
                cmd.timestamp = System.currentTimeMillis();
                cmd.action = placeOrder.getAction();
                cmd.orderType = placeOrder.getOrderType();
                cmd.symbol = specification.symbolId;
                cmd.uid = placeOrder.getUid();

                // TODO fix
                //userCookies.put(seq, placeOrder.getCookieId());
            });


            return U.map("status", "ok");

        });


    }


}
