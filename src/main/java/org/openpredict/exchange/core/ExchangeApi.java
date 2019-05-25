package org.openpredict.exchange.core;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.nustaq.serialization.FSTConfiguration;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

@RequiredArgsConstructor
@Slf4j
public final class ExchangeApi {

    private final ExchangeCore exchangeCore;

    private final static FSTConfiguration minBin = FSTConfiguration.createMinBinConfiguration();

    static {
        minBin.registerCrossPlatformClassMappingUseSimpleName(CoreSymbolSpecification.class);
    }


    public void submitCommand(ApiCommand cmd) {
        //log.debug("{}", cmd);
        final RingBuffer<OrderCommand> ringBuffer = exchangeCore.getRingBuffer();
        // TODO benchmark instanceof performance

        if (cmd instanceof ApiMoveOrder) {
            ringBuffer.publishEvent(MOVE_ORDER_TRANSLATOR, (ApiMoveOrder) cmd);
        } else if (cmd instanceof ApiPlaceOrder) {
            ringBuffer.publishEvent(NEW_ORDER_TRANSLATOR, (ApiPlaceOrder) cmd);
        } else if (cmd instanceof ApiCancelOrder) {
            ringBuffer.publishEvent(CANCEL_ORDER_TRANSLATOR, (ApiCancelOrder) cmd);
        } else if (cmd instanceof ApiOrderBookRequest) {
            ringBuffer.publishEvent(ORDER_BOOK_REQUEST_TRANSLATOR, (ApiOrderBookRequest) cmd);
        } else if (cmd instanceof ApiAddUser) {
            ringBuffer.publishEvent(ADD_USER_TRANSLATOR, (ApiAddUser) cmd);
        } else if (cmd instanceof ApiAdjustUserBalance) {
            ringBuffer.publishEvent(ADJUST_USER_BALANCE_TRANSLATOR, (ApiAdjustUserBalance) cmd);
        } else if (cmd instanceof ApiBinaryDataCommand) {
            publishBinaryData(ringBuffer, (ApiBinaryDataCommand) cmd);
        } else if (cmd instanceof ApiPersistState) {
            ringBuffer.publishEvent(PERSIST_STATE_TRANSLATOR, (ApiPersistState) cmd);
        } else if (cmd instanceof ApiReset) {
            ringBuffer.publishEvent(RESET_TRANSLATOR, (ApiReset) cmd);
        } else if (cmd instanceof ApiNoOp) {
            ringBuffer.publishEvent(NOOP_TRANSLATOR, (ApiNoOp) cmd);
        } else {
            throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }

    private void publishBinaryData(final RingBuffer<OrderCommand> ringBuffer, final ApiBinaryDataCommand apiCmd) {

        final byte[] bytes = minBin.asByteArray(apiCmd.data);

        // 1010011000 >> 1010011
        // 1010011001 >> 1010011 + 1
        // 1010011010 >> 1010011 + 1
        //       ..........
        // 1010011110 >> 1010011 + 1
        // 1010011111 >> 1010011 + 1

        // TODO optimize

        final int longLength = Utils.requiredLongArraySize(bytes.length);
        long[] longArray = new long[longLength];
        //log.debug("byte[{}]={}", bytes.length, bytes);


        final ByteBuffer allocate = ByteBuffer.allocate(bytes.length * 2);
        final LongBuffer longBuffer = allocate.asLongBuffer();
        allocate.put(bytes);
        longBuffer.get(longArray);
        //log.debug("longArray[{}]={}",longArray.length, longArray);

        int i = 0;
        long highSeq = ringBuffer.next(longLength);
        long lowSeq = highSeq - longLength + 1;

        try {
            for (long seq = lowSeq; seq <= highSeq; seq++) {

                OrderCommand cmd = ringBuffer.get(seq);
                cmd.command = OrderCommandType.BINARY_DATA;
                cmd.orderId = apiCmd.transferId;
                cmd.symbol = -1;
                cmd.price = longArray[i];
                cmd.size = ((long) bytes.length << 32) + i;
                cmd.uid = -1;
                cmd.timestamp = apiCmd.timestamp;
                cmd.resultCode = CommandResultCode.NEW;

                //log.debug("seq={} cmd.size={} data={}", seq, cmd.size, cmd.price);

                i++;
            }
        } catch (Exception ex) {
            log.error("Binary commands processing exception: ", ex);

        } finally {
            //System.out.println("publish " + lowSeq + "-" + highSeq);

            ringBuffer.publish(lowSeq, highSeq);
        }
    }

    private static final EventTranslatorOneArg<OrderCommand, ApiPlaceOrder> NEW_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.price = api.price;
        cmd.size = api.size;
        cmd.orderId = api.id;
        cmd.timestamp = api.timestamp;
        cmd.action = api.action;
        cmd.orderType = api.orderType;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiMoveOrder> MOVE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.price = api.newPrice;
        cmd.size = api.newSize;
        cmd.orderId = api.id;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiCancelOrder> CANCEL_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = api.id;
        cmd.price = -1;
        cmd.size = -1;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiOrderBookRequest> ORDER_BOOK_REQUEST_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
        cmd.orderId = -1;
        cmd.symbol = api.symbol;
        cmd.price = -1;
        cmd.size = api.size;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiAddUser> ADD_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.ADD_USER;
        cmd.orderId = -1;
        cmd.symbol = -1;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiAdjustUserBalance> ADJUST_USER_BALANCE_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
        cmd.orderId = api.transactionId;
        cmd.symbol = api.currency;
        cmd.uid = api.uid;
        cmd.price = api.amount;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiReset> RESET_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.RESET;
        cmd.orderId = -1;
        cmd.symbol = -1;
        cmd.uid = -1;
        cmd.price = -1;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiPersistState> PERSIST_STATE_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.PERSIST_STATE;
        cmd.orderId = api.dumpId;
        cmd.symbol = -1;
        cmd.uid = -1;
        cmd.price = -1;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiNoOp> NOOP_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.NOP;
        cmd.orderId = -1;
        cmd.symbol = -1;
        cmd.uid = -1;
        cmd.price = -1;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };
}
