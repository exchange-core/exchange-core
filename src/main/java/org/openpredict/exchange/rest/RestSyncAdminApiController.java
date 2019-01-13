package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.openpredict.exchange.rest.commands.admin.RestApiAccountBalanceAdjustment;
import org.openpredict.exchange.rest.commands.admin.RestApiAddSymbol;
import org.openpredict.exchange.rest.commands.admin.RestApiAddUser;
import org.rapidoid.http.Req;
import org.rapidoid.http.Resp;
import org.rapidoid.setup.On;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.Optional;

@Service
@Slf4j
public class RestSyncAdminApiController {

    @Autowired
    private AsyncAdminInterface asyncAdminInterface;

    @Autowired
    private GatewayState gatewayState;

    @PostConstruct
    public void initRestApi() {

//        App.bootstrap(new String[0]);

        On.get("/syncAdminApi/v1/symbols/{symbolName}/orderBook").json((Req req, String symbolName) -> {
            //log.info(">>> data={}", data);
            log.info(">>> symbolName={}", symbolName);

            Optional<GatewaySymbolSpecification> spec = gatewayState.getSymbolSpec(symbolName);
            Resp resp = req.response();
            if (!spec.isPresent()) {
                log.debug("Not present");
                resp.code(404);
                return null;
            }

            int symbolId = spec.get().symbolId;
            return gatewayState.doAsyncCall(req, ticket -> asyncAdminInterface.orderBookRequest(ticket, symbolId));
        });


        On.post("/syncAdminApi/v1/users").json((Req req, RestApiAddUser addUser) -> {
            log.info(">>> {}", addUser);
            return gatewayState.doAsyncCall(req, ticket ->
                    asyncAdminInterface.createUser(ticket, addUser.getUid()));
        });

        On.post("/syncAdminApi/v1/users/balance").json((Req req, RestApiAccountBalanceAdjustment adjustment) -> {
            log.info(">>> {}", adjustment);

            // TODO currency conversion
            final BigDecimal amount = new BigDecimal(adjustment.getAmount());
            final long longAmount = amount.longValue();

            return gatewayState.doAsyncCall(req, ticket ->
                    asyncAdminInterface.balanceAdjustment(ticket, adjustment.getUid(), adjustment.getTransactionId(), longAmount));
        });


        On.post("/syncAdminApi/v1/symbols").json((Req req, RestApiAddSymbol addSymbol) -> {
            log.info(">>> {}", addSymbol);

            // TODO Publish through bus

            GatewaySymbolSpecification spec = GatewaySymbolSpecification.builder()
                    .symbolId(addSymbol.getSymbolId())
                    .symbolName(addSymbol.getSymbolName())
                    .lotSize(addSymbol.getLotSize())
                    .priceScale(addSymbol.getPriceScale())
                    .priceStep(addSymbol.getPriceStep())
                    .active(false)
                    .build();

            gatewayState.registerSymbolIfNotActive(spec);

            return gatewayState.doAsyncCall(req, ticket ->
                    asyncAdminInterface.addSymbol(
                            ticket,
                            addSymbol.getSymbolId(),
                            addSymbol.getDepositBuy(),
                            addSymbol.getDepositSell(),
                            addSymbol.getPriceLowLimit(),
                            addSymbol.getPriceHighLimit()));
        });

    }


}
