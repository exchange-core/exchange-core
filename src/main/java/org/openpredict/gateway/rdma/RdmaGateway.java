package org.openpredict.gateway.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPostRecv;
import com.ibm.disni.verbs.SVCPostSend;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.benchmarking.TestOrdersGenerator;
import org.openpredict.exchange.beans.cmd.OrderCommandType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class RdmaGateway implements RdmaEndpointFactory<ExchangeRdmaClientEndpoint> {
    private RdmaActiveEndpointGroup<ExchangeRdmaClientEndpoint> endpointGroup;

    private final int bufferSize = 64;

    private String host = "192.168.7.2";
    private Integer port = 1919;

    public static final int SYMBOL = 5512;


    public ExchangeRdmaClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new ExchangeRdmaClientEndpoint(endpointGroup, idPriv, serverSide, bufferSize);
    }

    public void runTest() throws Exception {

        log.info("starting...");

        List<Long> uids = Stream.iterate(1L, i -> i + 1).limit(10).collect(Collectors.toList());

        CompletableFuture<TestOrdersGenerator.GenResult> genFuture = CompletableFuture.supplyAsync(() -> {
            TestOrdersGenerator testOrdersGenerator = new TestOrdersGenerator();
            return testOrdersGenerator.generateCommands(10000, 1000, uids, SYMBOL, false);
        });

        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);

        ExchangeRdmaClientEndpoint endpoint = endpointGroup.createEndpoint();

        endpoint.connect(new InetSocketAddress(InetAddress.getByName(host), port), 1000);
        log.info("client channel set up ");

        ExchangeApiClient apiClient = new ExchangeApiClient(endpoint);

        log.info("creating symbol...");
        apiClient.sendData(new long[]{((long) SYMBOL << 32) + ((long) 1 << 8) + 50, System.nanoTime(), -1, -1, 1500});


        uids.forEach(uid -> {
            log.info("creating user {}", uid);
            apiClient.sendData(new long[]{10, System.nanoTime(), uid});

            log.info("set balance...");
            apiClient.sendData(new long[]{11, System.nanoTime(), uid, -1, 2_000_000});
        });


        genFuture.get().getCommands().forEach(cmd -> {
            log.info("send order: {}", cmd);
            final long t = System.nanoTime();
            if (cmd.command == OrderCommandType.PLACE_ORDER) {
                byte askBid = cmd.action.getCode();
                byte limitMarket = cmd.orderType.getCode();
                apiClient.sendData(new long[]{((long) SYMBOL << 32) + 1, t, cmd.uid, cmd.orderId, cmd.price, cmd.size, (limitMarket << 8) + askBid});
            } else if (cmd.command == OrderCommandType.MOVE_ORDER) {
                apiClient.sendData(new long[]{((long) SYMBOL << 32) + 3, t, cmd.uid, cmd.orderId, cmd.price, cmd.size});
            } else if (cmd.command == OrderCommandType.CANCEL_ORDER) {
                apiClient.sendData(new long[]{((long) SYMBOL << 32) + 2, t, cmd.uid, cmd.orderId});
            }
        });

//        log.info("send order...");
//        int orderId = 8162;
//        int price = 40000;
//        int size = 5;
//        byte askBid = OrderAction.ASK.getCode();
//        byte limitMarket = OrderType.MARKET.getCode();
//        apiClient.sendData(new long[]{((long) SYMBOL << 32) + 1, System.nanoTime(), uid, orderId, price, size, (limitMarket << 8) + askBid});

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


        private void sendData(long[] data) {
            try {
                // send...
                sendBuffer.asLongBuffer().put(data);

                sendBuffer.clear();

                postSendStatefulVerbCall.execute();


                endpoint.getWorkCompletionEvents().take();

                // receive...
                postReceiveStatefulVerbCall.execute();
                endpoint.getWorkCompletionEvents().take();
                receiveBuffer.clear();

            } catch (IOException | InterruptedException ex) {
                throw new IllegalStateException(ex);
            }

        }

    }


    public static void main(String[] args) throws Exception {
        RdmaGateway gateway = new RdmaGateway();
        gateway.runTest();
        System.exit(0);
    }
}

