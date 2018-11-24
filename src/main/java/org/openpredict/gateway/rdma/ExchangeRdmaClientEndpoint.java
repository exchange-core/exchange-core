package org.openpredict.gateway.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public final class ExchangeRdmaClientEndpoint extends RdmaActiveEndpoint {
    //private ByteBuffer dataBuffer;
    private ByteBuffer sendBuffer;
    private ByteBuffer receiveBuffer;

    private LinkedList<IbvSendWR> sendWorkRequests;
    private LinkedList<IbvSge> sendScatterGatherElements;

    private LinkedList<IbvRecvWR> receiveWorkRequests;
    private LinkedList<IbvSge> receiveScatterGatherElements;

    private ArrayBlockingQueue<IbvWC> workCompletionEvents;


    public ExchangeRdmaClientEndpoint(RdmaActiveEndpointGroup<ExchangeRdmaClientEndpoint> endpointGroup,
                                      RdmaCmId idPriv, boolean serverSide, int bufferSize) throws IOException {
        super(endpointGroup, idPriv, serverSide);

        //this.dataBuffer = ByteBuffer.allocateDirect(bufferSize);
        this.sendBuffer = ByteBuffer.allocateDirect(bufferSize);
        this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize);

        this.sendWorkRequests = new LinkedList<>();
        this.sendScatterGatherElements = new LinkedList<>();

        this.receiveWorkRequests = new LinkedList<>();
        this.receiveScatterGatherElements = new LinkedList<>();

        this.workCompletionEvents = new ArrayBlockingQueue<>(10);
    }

    public void init() throws IOException {
        super.init();

        //IbvMr dataMr = registerMemory(dataBuffer).execute().free().getMr();
        IbvMr sendMr = registerMemory(sendBuffer).execute().free().getMr();
        IbvMr receiveMr = registerMemory(receiveBuffer).execute().free().getMr();

//        sendBuffer.putLong(dataMr.getAddr());
//        sendBuffer.putInt(dataMr.getLength());
//        sendBuffer.putInt(dataMr.getLkey());
//        sendBuffer.clear();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(sendMr.getAddr());
        sgeSend.setLength(sendMr.getLength());
        sgeSend.setLkey(sendMr.getLkey());
        sendScatterGatherElements.add(sgeSend);

        final IbvSendWR sendWorkRequest = new IbvSendWR();
        sendWorkRequest.setWr_id(2000);
        sendWorkRequest.setSg_list(sendScatterGatherElements);
        sendWorkRequest.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWorkRequest.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWorkRequests.add(sendWorkRequest);


        IbvSge sgeReceive = new IbvSge();
        sgeReceive.setAddr(receiveMr.getAddr());
        sgeReceive.setLength(receiveMr.getLength());
        int lkey = receiveMr.getLkey();
        sgeReceive.setLkey(lkey);
        receiveScatterGatherElements.add(sgeReceive);

        final IbvRecvWR receiveWorkRequest = new IbvRecvWR();
        receiveWorkRequest.setSg_list(receiveScatterGatherElements);
        receiveWorkRequest.setWr_id(2001);
        receiveWorkRequests.add(receiveWorkRequest);

        this.postRecv(receiveWorkRequests).execute().free();
    }

    public void dispatchCqEvent(IbvWC wc) {
        workCompletionEvents.add(wc);
    }

    public ArrayBlockingQueue<IbvWC> getWorkCompletionEvents() {
        return workCompletionEvents;
    }

    public LinkedList<IbvSendWR> getSendWorkRequests() {
        return sendWorkRequests;
    }

    public LinkedList<IbvRecvWR> getReceiveWorkRequests() {
        return receiveWorkRequests;
    }

    public ByteBuffer getSendBuffer() {
        return sendBuffer;
    }

    public ByteBuffer getReceiveBuffer() {
        return receiveBuffer;
    }

}
