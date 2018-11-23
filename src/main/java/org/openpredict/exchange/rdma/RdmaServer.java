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

                ringBuffer.publishEvent((cmd, seq) -> {

                    final long word = longRcvBuffer.get(0);
                    final OrderCommandType commandType = OrderCommandType.valueOf((byte) (word & 0x7f));
                    cmd.command = commandType;
                    cmd.resultCode = CommandResultCode.NEW;

                    log.debug("Received command: {}", commandType);

                    if (commandType == OrderCommandType.PLACE_ORDER) {
                        cmd.timestamp = longRcvBuffer.get(1);
                        cmd.uid = longRcvBuffer.get(2);
                        cmd.orderId = longRcvBuffer.get(3);
                        cmd.price = longRcvBuffer.get(4);
                        cmd.size = longRcvBuffer.get(5);
                        cmd.action = OrderAction.ASK;
                        cmd.orderType = OrderType.MARKET;
                        cmd.symbol = (int) longRcvBuffer.get(6);

                    } else if (commandType == OrderCommandType.ADD_USER) {
                        cmd.timestamp = longRcvBuffer.get(1);
                        cmd.uid = longRcvBuffer.get(2);
                        cmd.orderId = -1;
                        cmd.symbol = -1;
                        cmd.resultCode = CommandResultCode.NEW;

                    } else if (commandType == OrderCommandType.BALANCE_ADJUSTMENT) {
                        cmd.timestamp = longRcvBuffer.get(1);
                        cmd.uid = longRcvBuffer.get(2);
                        cmd.orderId = -1;
                        cmd.price = longRcvBuffer.get(4);
                        cmd.resultCode = CommandResultCode.NEW;

                    } else if (commandType == OrderCommandType.ADD_SYMBOL) {
                        cmd.timestamp = longRcvBuffer.get(1);
                        cmd.orderId = -1;
                        cmd.symbol = (int) longRcvBuffer.get(6);
                        cmd.price = longRcvBuffer.get(4);
                        cmd.resultCode = CommandResultCode.NEW;

                    } else {
                        throw new UnsupportedOperationException("Not supported command");
                    }

                });

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
}
