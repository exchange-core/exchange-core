package org.openpredict.gateway.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPostRecv;
import com.ibm.disni.verbs.SVCPostSend;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

@Slf4j
public class RdmaGateway implements RdmaEndpointFactory<ExchangeRdmaClientEndpoint> {
    private RdmaActiveEndpointGroup<ExchangeRdmaClientEndpoint> endpointGroup;

    private int bufferSize = 64;

    private String host = "192.168.7.2";
    private Integer port = 1919;


    public ExchangeRdmaClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new ExchangeRdmaClientEndpoint(endpointGroup, idPriv, serverSide, bufferSize);
    }

    public void runTest() throws Exception {

        log.info("starting...");

        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);

        ExchangeRdmaClientEndpoint endpoint = endpointGroup.createEndpoint();

        endpoint.connect(new InetSocketAddress(InetAddress.getByName(host), port), 1000);
        log.info("client channel set up ");

        ExchangeApiClient apiClient = new ExchangeApiClient(endpoint);

        log.info("creating symbol...");
        final int symbol = 5512;
        apiClient.sendData(new long[]{((long) symbol << 32) + ((long) 1 << 8) + 50, System.nanoTime(), -1, -1, 1500});

        final int uid = 10001;

        log.info("creating user...");
        apiClient.sendData(new long[]{10, System.nanoTime(), uid});

        log.info("set balance...");
        apiClient.sendData(new long[]{11, System.nanoTime(), uid, -1, 2_000_000});

        log.info("send order...");
        int orderId = 8162;
        int price = 40000;
        int size = 5;
        int askBid = 0;
        int limitMarket = 0;
        apiClient.sendData(new long[]{((long) symbol << 32) + 1, System.nanoTime(), uid, orderId, price, size, limitMarket * 2 + askBid});

        endpoint.close();
        endpointGroup.close();
    }

    public class ExchangeApiClient {
        private final ByteBuffer sendBuffer;
        private final ByteBuffer receiveBuffer;

        private final SVCPostSend postSendStatefulVerbCall;
        private final SVCPostRecv postReceiveStatefulVerbCall;

        private final ExchangeRdmaClientEndpoint endpoint;

        public ExchangeApiClient(ExchangeRdmaClientEndpoint endpoint) throws IOException {
            this.endpoint = endpoint;
            sendBuffer = endpoint.getSendBuffer();
            receiveBuffer = endpoint.getReceiveBuffer();

            postSendStatefulVerbCall = endpoint.postSend(endpoint.getSendWorkRequests());
            postReceiveStatefulVerbCall = endpoint.postRecv(endpoint.getReceiveWorkRequests());
        }


        private void sendData(long[] data) throws IOException, InterruptedException {
            // send...
            sendBuffer.asLongBuffer().put(data);

            sendBuffer.clear();
            postSendStatefulVerbCall.execute();
            endpoint.getWorkCompletionEvents().take();

            // receive...
            postReceiveStatefulVerbCall.execute();
            endpoint.getWorkCompletionEvents().take();
            receiveBuffer.clear();
        }

    }


    public static void main(String[] args) throws Exception {
        RdmaGateway gateway = new RdmaGateway();
        gateway.runTest();
        System.exit(0);
    }
}

