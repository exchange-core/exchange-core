package org.openpredict.exchange.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class Endpoint extends RdmaActiveEndpoint {

    private ByteBuffer dataBuffer;
    private ByteBuffer sendBuffer;
    private ByteBuffer receiveBuffer;

    private LinkedList<IbvSendWR> wrListSend;
    private IbvSge sgeSend;
    private LinkedList<IbvSge> sgeList;
    private IbvSendWR sendWR;

    private LinkedList<IbvRecvWR> wrListRecv;
    private IbvSge sgeRecv;
    private LinkedList<IbvSge> sgeListRecv;
    private IbvRecvWR recvWR;

    private ArrayBlockingQueue<IbvWC> wcEvents;

    public Endpoint(RdmaActiveEndpointGroup<Endpoint> endpointGroup,
                    RdmaCmId idPriv, boolean serverSide, int buffersize) throws IOException {

        super(endpointGroup, idPriv, serverSide);


        this.dataBuffer = ByteBuffer.allocateDirect(buffersize);
        this.sendBuffer = ByteBuffer.allocateDirect(buffersize);
        this.receiveBuffer = ByteBuffer.allocateDirect(buffersize);


        this.wrListSend = new LinkedList<>();
        this.sgeSend = new IbvSge();
        this.sgeList = new LinkedList<>();
        this.sendWR = new IbvSendWR();

        this.wrListRecv = new LinkedList<>();
        this.sgeRecv = new IbvSge();
        this.sgeListRecv = new LinkedList<>();
        this.recvWR = new IbvRecvWR();

        this.wcEvents = new ArrayBlockingQueue<>(10);
    }

    public void init() throws IOException{
        super.init();

        //IbvMr dataMr = registerMemory(dataBuffer).execute().free().getMr();
        IbvMr sendMr = registerMemory(sendBuffer).execute().free().getMr();
        IbvMr receiveMr = registerMemory(receiveBuffer).execute().free().getMr();

        sgeSend.setAddr(sendMr.getAddr());
        sgeSend.setLength(sendMr.getLength());
        sgeSend.setLkey(sendMr.getLkey());
        sgeList.add(sgeSend);
        sendWR.setWr_id(2000);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wrListSend.add(sendWR);

        sgeRecv.setAddr(receiveMr.getAddr());
        sgeRecv.setLength(receiveMr.getLength());
        int lkey = receiveMr.getLkey();
        sgeRecv.setLkey(lkey);
        sgeListRecv.add(sgeRecv);
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(2001);
        wrListRecv.add(recvWR);

        this.postRecv(wrListRecv).execute().free();
    }

    public void dispatchCqEvent(IbvWC wc) {
        wcEvents.add(wc);
    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        return wcEvents;
    }

    public LinkedList<IbvSendWR> getWrListSend() {
        return wrListSend;
    }

    public LinkedList<IbvRecvWR> getWrListRecv() {
        return wrListRecv;
    }

    public ByteBuffer getDataBuffer() {
        return dataBuffer;
    }

    public ByteBuffer getSendBuffer() {
        return sendBuffer;
    }

    public ByteBuffer getReceiveBuffer() {
        return receiveBuffer;
    }

}