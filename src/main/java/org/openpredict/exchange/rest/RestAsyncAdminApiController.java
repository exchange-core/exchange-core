package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.openpredict.exchange.rest.commands.admin.RestApiAccountBalanceAdjustment;
import org.openpredict.exchange.rest.commands.admin.RestApiAddSymbol;
import org.openpredict.exchange.rest.commands.admin.RestApiAddUser;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.ExchangeCore;
import org.rapidoid.setup.App;
import org.rapidoid.setup.On;
import org.rapidoid.u.U;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;

@Service
@Slf4j
public class RestAsyncAdminApiController {

    @Autowired
    private ExchangeCore exchangeCore;

    @Autowired
    private GatewayState gatewayState;

    @PostConstruct
    public void initRestApi() {
        App.bootstrap(new String[0]);

//        On.post("/asyncTradeApi/v1/test").json((ApiTestObject testObject) -> {
//            log.info(">>> {}", testObject);
//            return U.map("msg", "ok");
//        });

        // TODO change to users + accounts

        On.post("/asyncAdminApi/v1/users").json((RestApiAddUser addUser) -> {
            log.info(">>> {}", addUser);

            exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
                cmd.command = OrderCommandType.ADD_USER;
                cmd.orderId = -1;
                cmd.symbol = -1;
                cmd.uid = addUser.getUid();
                cmd.timestamp = System.currentTimeMillis();
                cmd.resultCode = CommandResultCode.NEW;
            }));

            return U.map("msg", "ok");

        });


        On.post("/asyncAdminApi/v1/users/balance").json((RestApiAccountBalanceAdjustment adjustment) -> {
            log.info(">>> {}", adjustment);

            // TODO fix conversion
            final BigDecimal amount = new BigDecimal(adjustment.getAmount());
            final long longAmount = amount.longValue();

            exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
                cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
                cmd.orderId = adjustment.getTransactionId();
                cmd.symbol = -1;
                cmd.uid = adjustment.getUid();
                cmd.price = longAmount;
                cmd.size = 0;
                cmd.timestamp = System.currentTimeMillis();
                cmd.resultCode = CommandResultCode.NEW;
            }));

            return U.map("msg", "ok");
        });


        // TODO merge symbols api

        On.post("/asyncAdminApi/v1/symbols").json((RestApiAddSymbol addSymbol) -> {
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

            exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
                cmd.command = OrderCommandType.ADD_SYMBOL;
                cmd.symbol = addSymbol.getSymbolId();

                cmd.price = addSymbol.getDepositBuy();
                cmd.uid = addSymbol.getDepositSell();
                cmd.orderId = addSymbol.getPriceLowLimit();
                cmd.size = addSymbol.getPriceHighLimit();

                cmd.timestamp = System.currentTimeMillis();
                cmd.resultCode = CommandResultCode.NEW;
            }));

            return U.map("msg", "ok");
        });


    }


}
