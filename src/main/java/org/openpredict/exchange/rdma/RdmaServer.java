package org.openpredict.exchange.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPostRecv;
import com.ibm.disni.verbs.SVCPostSend;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.beans.cmd.SymbolCommandSubType;
import org.openpredict.exchange.core.ExchangeCore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.CompletableFuture;

import static org.openpredict.exchange.rdma.RdmaApiConstants.*;

@Service
@Slf4j
public class RdmaServer implements RdmaEndpointFactory<Endpoint> {


    @Autowired
    private ExchangeCore exchangeCore;

    private RdmaActiveEndpointGroup<Endpoint> epg;
    private int bufferSize = 64;

    private RdmaServerEndpoint<Endpoint> ep;

    private String host = "192.168.7.2";
    private Integer port = 1919;


    @PostConstruct
    public void init() {
        CompletableFuture.runAsync(this::launch);

    }

    @Override
    public Endpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new Endpoint(epg, id, serverSide, bufferSize);
    }


    private void launch() {
        try {

            epg = new RdmaActiveEndpointGroup<>(5000, false, 128, 4, 128);
            epg.init(this);
            ep = epg.createServerEndpoint();
            ep.bind(new InetSocketAddress(InetAddress.getByName(host), port), 10);
            Endpoint clientEndpoint = ep.accept();

            RingBuffer<OrderCommand> ringBuffer = exchangeCore.getRingBuffer();

            log.info("RDMA client connected");
            ByteBuffer sendBuf = clientEndpoint.getSendBuffer();
            ByteBuffer recvBuf = clientEndpoint.getReceiveBuffer();
            SVCPostSend postSend = clientEndpoint.postSend(clientEndpoint.getWrListSend());
            SVCPostRecv postRecv = clientEndpoint.postRecv(clientEndpoint.getWrListRecv());
            LongBuffer longRcvBuffer = recvBuf.asLongBuffer();
            int c = 0;
            do {
                postRecv.execute();
                IbvWC wc = clientEndpoint.getWcEvents().take();

                log.debug("WorkCompletion: id:{} bytes:{} wq-idx:{} status:{} opcode:{}",
                        wc.getWr_id(), wc.getByte_len(), wc.getWqIndex(), wc.getStatus(), wc.getOpcode());

                recvBuf.clear();

                ringBuffer.publishEvent((OrderCommand cmd, long seq) -> putCommandIntoRingBuffer(longRcvBuffer, cmd));

                LongBuffer longSendBuffer = sendBuf.asLongBuffer();
                longSendBuffer.put(longRcvBuffer.get(0));
                longSendBuffer.put(longRcvBuffer.get(1));

                postSend.execute();
                clientEndpoint.getWcEvents().take();
                sendBuf.clear();

                Thread.sleep(2000);

            } while (c++ < 3_000_000);

            clientEndpoint.close();
            ep.close();
            epg.close();
        } catch (Exception e) {
            log.error("Cannot start RDMA server", e);
        }
    }

    private void putCommandIntoRingBuffer(LongBuffer longRcvBuffer, OrderCommand cmd) {
        final long headerWord = longRcvBuffer.get(CMD_HEADER);
        final OrderCommandType commandType = OrderCommandType.valueOf((byte) (headerWord & 0x7f));

        cmd.symbol = (int) ((headerWord >> 32) & 0x7fff);
        cmd.command = commandType;
        cmd.resultCode = CommandResultCode.NEW;
        cmd.timestamp = longRcvBuffer.get(CMD_TIMESTAMP);
        cmd.uid = longRcvBuffer.get(CMD_UID);

        log.debug("Received command: {}", commandType);

        if (commandType == OrderCommandType.PLACE_ORDER) {
            cmd.orderId = longRcvBuffer.get(CMD_ORDER_ID);
            cmd.price = longRcvBuffer.get(CMD_PRICE);
            cmd.size = longRcvBuffer.get(CMD_SIZE);
            long placeOrderFlags = longRcvBuffer.get(CMD_PLACEORDER_FLAGS);
            cmd.action = OrderAction.valueOf(placeOrderFlags & CMD_PLACEORDER_FLAGS_ACTION_MASK);
            cmd.orderType = OrderType.valueOf(placeOrderFlags & CMD_PLACEORDER_FLAGS_TYPE_MASK);

        } else if (commandType == OrderCommandType.MOVE_ORDER) {
            cmd.orderId = longRcvBuffer.get(CMD_ORDER_ID);
            cmd.price = longRcvBuffer.get(CMD_PRICE);
            cmd.size = longRcvBuffer.get(CMD_SIZE);

        } else if (commandType == OrderCommandType.CANCEL_ORDER) {
            cmd.orderId = longRcvBuffer.get(CMD_ORDER_ID);

        } else if (commandType == OrderCommandType.ADD_USER) {
            //

        } else if (commandType == OrderCommandType.BALANCE_ADJUSTMENT) {
            cmd.price = longRcvBuffer.get(CMD_PRICE);
            cmd.resultCode = CommandResultCode.NEW;

        } else if (commandType == OrderCommandType.SYMBOL_COMMANDS) {

            byte subCommandCode = (byte) ((headerWord >> 8) & 0x7f);
            cmd.subCommandCode = subCommandCode;
            SymbolCommandSubType subCommand = SymbolCommandSubType.valueOf(subCommandCode);
            if (subCommand == SymbolCommandSubType.ADD_SYMBOL) {
                cmd.price = longRcvBuffer.get(CMD_PRICE);
            } else {
                // TODO Implement
                throw new UnsupportedOperationException("Not supported sub-command: " + subCommand);
            }
            cmd.resultCode = CommandResultCode.NEW;

        } else {
            throw new UnsupportedOperationException("Not supported command: " + commandType);
        }
    }
}
