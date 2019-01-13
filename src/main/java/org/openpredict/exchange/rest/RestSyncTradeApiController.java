package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.openpredict.exchange.core.ExchangeCore;
import org.openpredict.exchange.rest.commands.RestApiCancelOrder;
import org.openpredict.exchange.rest.commands.RestApiMoveOrder;
import org.openpredict.exchange.rest.commands.RestApiPlaceOrder;
import org.rapidoid.http.Req;
import org.rapidoid.setup.On;
import org.rapidoid.u.U;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.Optional;

@Service
@Slf4j
public class RestSyncTradeApiController {

    @Autowired
    private ExchangeCore exchangeCore;

    @Autowired
    private GatewayState gatewayState;

    @Autowired
    private AsyncTradingInterface asyncTradingInterface;

    // TODO per user
    //private ConcurrentHashMap<Long, Long> userCookies = new ConcurrentHashMap<>();

    @PostConstruct
    public void initRestApi() {


        On.post("/syncTradeApi/v1/orders").json((Req req, RestApiPlaceOrder placeOrder) -> {
            log.info(">>> ", placeOrder);
            //exchangeApi.submitCommand(placeOrder);


            final Optional<GatewaySymbolSpecification> specOpt = gatewayState.getSymbolSpec(placeOrder.getSymbol());
            if (!specOpt.isPresent()) {
                return U.map("status", "failed", "description", "unknown symbol");
            }
            final GatewaySymbolSpecification specification = specOpt.get();
            final int symbolId = specification.symbolId;

            final BigDecimal price = new BigDecimal(placeOrder.getPrice());
            final long longPrice = price.longValue();

            final BigDecimal size = new BigDecimal(placeOrder.getSize());
            final long longSize = size.longValue();

            return gatewayState.doAsyncCall(req, ticket ->
                    asyncTradingInterface.placeNewOrder(
                            ticket, longPrice, longSize, placeOrder.getAction(), placeOrder.getOrderType(), symbolId, placeOrder.getUid()));
        });

        On.put("/syncTradeApi/v1/orders").json((Req req, RestApiMoveOrder moveOrder) -> {
            log.info(">>> ", moveOrder);

            final Optional<GatewaySymbolSpecification> specOpt = gatewayState.getSymbolSpec(moveOrder.getSymbol());
            if (!specOpt.isPresent()) {
                return U.map("status", "failed", "description", "unknown symbol");
            }
            final GatewaySymbolSpecification specification = specOpt.get();
            final int symbolId = specification.symbolId;

            final BigDecimal price = new BigDecimal(moveOrder.getPrice());
            final long longPrice = price.longValue();

            final BigDecimal size = new BigDecimal(moveOrder.getSize());
            final long longSize = size.longValue();

            return gatewayState.doAsyncCall(req, ticket ->
                    asyncTradingInterface.moveOrder(
                            ticket, longPrice, longSize, moveOrder.getOrderId(), symbolId, moveOrder.getUid()));
        });

        On.delete("/syncTradeApi/v1/orders").json((Req req, RestApiCancelOrder cancelOrder) -> {
            log.info(">>> ", cancelOrder);

            final Optional<GatewaySymbolSpecification> specOpt = gatewayState.getSymbolSpec(cancelOrder.getSymbol());
            if (!specOpt.isPresent()) {
                return U.map("status", "failed", "description", "unknown symbol");
            }
            final GatewaySymbolSpecification specification = specOpt.get();
            final int symbolId = specification.symbolId;

            return gatewayState.doAsyncCall(req, ticket ->
                    asyncTradingInterface.cancelOrder(
                            ticket, cancelOrder.getOrderId(), symbolId, cancelOrder.getUid()));
        });

    }


}
