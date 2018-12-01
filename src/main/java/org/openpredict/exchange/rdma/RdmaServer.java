package org.openpredict.exchange.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.*;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.cmd.OrderCommand;
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
    private final int bufferSize = 64;

    private RdmaServerEndpoint<Endpoint> ep;

    private String host = "192.168.7.2";
    private Integer port = 1919;

    boolean waitReceive = true;

    boolean active;

    RingBuffer<OrderCommand> ringBuffer;

    ByteBuffer recvBuf;
    ByteBuffer sendBuf;

    SVCPostSend postSend;
    SVCPostRecv postRecv;


    @PostConstruct
    public void init() {
        CompletableFuture.runAsync(this::launch);

    }

    @Override
    public Endpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new Endpoint(epg, id, serverSide, bufferSize, this::workCompetion, this::getDisconnected);
    }

    private void getDisconnected(RdmaCmEvent cmEvent) {
        if (cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
            log.info("DISCONNECTED");
            active = false;
        }
    }


    private void workCompetion(IbvWC wc) {
        try {
            log.debug("WorkCompletion: id:{} bytes:{} wq-idx:{} status:{} opcode:{}",
                    wc.getWr_id(), wc.getByte_len(), wc.getWqIndex(), wc.getStatus(), wc.getOpcode());

            if (waitReceive) {

                recvBuf.clear();

                LongBuffer longRcvBuffer = recvBuf.asLongBuffer();
                log.debug("longRcvBuffer:::: {}", longRcvBuffer);

                ringBuffer.publishEvent((OrderCommand cmd, long seq) -> putCommandIntoRingBuffer(longRcvBuffer, cmd));

                LongBuffer longSendBuffer = sendBuf.asLongBuffer();
                longSendBuffer.put(longRcvBuffer.get(0));
                longSendBuffer.put(longRcvBuffer.get(1));

                postSend.execute();
                waitReceive = false;
            } else {

                sendBuf.clear();

                postRecv.execute();
                waitReceive = true;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void launch() {
        try {

            epg = new RdmaActiveEndpointGroup<>(5000, false, 128, 4, 128);
            epg.init(this);
            ep = epg.createServerEndpoint();
            ep.bind(new InetSocketAddress(InetAddress.getByName(host), port), 10);

            active = true;

            Endpoint clientEndpoint = ep.accept();

            boolean waitReceive = true;

            ringBuffer = exchangeCore.getRingBuffer();

            log.info("RDMA client connected");
            sendBuf = clientEndpoint.getSendBuffer();
            recvBuf = clientEndpoint.getReceiveBuffer();
            postSend = clientEndpoint.postSend(clientEndpoint.getWrListSend());
            postRecv = clientEndpoint.postRecv(clientEndpoint.getWrListRecv());
            //int c = 0;

            postRecv.execute();

//            do {
//                log.debug("-------- {} --------------", c);
//
//                postRecv.execute();
//
//                log.debug("After postRecv.execute()");
//                IbvWC wc = clientEndpoint.getWcEvents().take();
//                log.debug("WorkCompletion: id:{} bytes:{} wq-idx:{} status:{} opcode:{}",
//                        wc.getWr_id(), wc.getByte_len(), wc.getWqIndex(), wc.getStatus(), wc.getOpcode());
//
//                recvBuf.clear();
//
//                log.debug("longRcvBuffer:::: {}", longRcvBuffer);
//
//                ringBuffer.publishEvent((OrderCommand cmd, long seq) -> putCommandIntoRingBuffer(longRcvBuffer, cmd));
//
//                LongBuffer longSendBuffer = sendBuf.asLongBuffer();
//                longSendBuffer.put(longRcvBuffer.get(0));
//                longSendBuffer.put(longRcvBuffer.get(1));
//
//                postSend.execute();
//                clientEndpoint.getWcEvents().take();
//                sendBuf.clear();
//
//                //Thread.sleep(1000);
//
//            } while (c++ < 3_000_000);


            while (active) {
                Thread.sleep(1000);
            }

            log.info("Close");

            clientEndpoint.close();
            ep.close();
            epg.close();
        } catch (Exception e) {
            log.error("Cannot start RDMA server", e);
        }
    }

    private void putCommandIntoRingBuffer(LongBuffer longRcvBuffer, OrderCommand cmd) {
        log.debug("Write data: {}", longRcvBuffer);
        cmd.readFromLongBuffer(longRcvBuffer);
        log.debug("Received command: {}", cmd.command);
    }
}

