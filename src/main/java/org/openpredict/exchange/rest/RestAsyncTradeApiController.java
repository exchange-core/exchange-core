package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.openpredict.exchange.beans.api.rest.RestApiCancelOrder;
import org.openpredict.exchange.beans.api.rest.RestApiMoveOrder;
import org.openpredict.exchange.beans.api.rest.RestApiPlaceOrder;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
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


        On.post("/asyncTradeApi/v1/orders").json((RestApiPlaceOrder placeOrder) -> {
            log.info(">>> ", placeOrder);
            //exchangeApi.submitCommand(placeOrder);


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
            exchangeCore.getRingBuffer().publishEvent((cmd, seq) -> {

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

        On.put("/asyncTradeApi/v1/orders").json((RestApiMoveOrder moveOrder) -> {
            log.info(">>> ", moveOrder);

            final Optional<GatewaySymbolSpecification> specOpt = gatewayState.getSymbolId(moveOrder.getSymbol());
            if (!specOpt.isPresent()) {
                return U.map("status", "failed", "description", "unknown symbol");
            }
            final GatewaySymbolSpecification specification = specOpt.get();

            final BigDecimal price = new BigDecimal(moveOrder.getPrice());
            final long longPrice = price.longValue();

            final BigDecimal size = new BigDecimal(moveOrder.getSize());
            final long longSize = size.longValue();


            // TODO chose proper exchange core instance based on symbol
            exchangeCore.getRingBuffer().publishEvent((cmd, seq) -> {

                cmd.command = OrderCommandType.MOVE_ORDER;
                cmd.resultCode = CommandResultCode.NEW;

                cmd.price = longPrice;
                cmd.size = longSize;
                cmd.orderId = moveOrder.getOrderId();
                cmd.timestamp = System.currentTimeMillis();
                cmd.symbol = specification.symbolId;
                cmd.uid = moveOrder.getUid();
            });

            return U.map("status", "ok");
        });

        On.delete("/asyncTradeApi/v1/orders").json((RestApiCancelOrder cancelOrder) -> {
            log.info(">>> ", cancelOrder);

            final Optional<GatewaySymbolSpecification> specOpt = gatewayState.getSymbolId(cancelOrder.getSymbol());
            if (!specOpt.isPresent()) {
                return U.map("status", "failed", "description", "unknown symbol");
            }
            final GatewaySymbolSpecification specification = specOpt.get();

            // TODO chose proper exchange core instance based on symbol
            exchangeCore.getRingBuffer().publishEvent((cmd, seq) -> {

                cmd.command = OrderCommandType.CANCEL_ORDER;
                cmd.resultCode = CommandResultCode.NEW;

                cmd.orderId = cancelOrder.getOrderId();
                cmd.timestamp = System.currentTimeMillis();
                cmd.symbol = specification.symbolId;
                cmd.uid = cancelOrder.getUid();
            });

            return U.map("status", "ok");
        });


    }


}
