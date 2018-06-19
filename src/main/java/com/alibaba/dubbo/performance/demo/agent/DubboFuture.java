package com.alibaba.dubbo.performance.demo.agent;

import java.io.IOException;
import java.util.Map;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.component.NioSocketChannel;
import com.generallycloud.baseio.protocol.AbstractFuture;

public final class DubboFuture extends AbstractFuture {
    private long                id;
    private boolean             isRequest;
    private int                 serializationId = 6;
    private int                 status;
    private boolean             twoWay          = true;
    private boolean             event           = false;
    private String              dubboVersion    = "2.6.0";
    private String              serviceVersion  = "0.0.0";
    private Map<String, String> attachments;
    private ByteBuf              data;

    public String getDubboVersion() {
        return dubboVersion;
    }

    public String getServiceVersion() {
        return serviceVersion;
    }

    public Map<String, String> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, String> attachments) {
        this.attachments = attachments;
    }
    
    public void setDubboVersion(String dubboVersion) {
        this.dubboVersion = dubboVersion;
    }

    public void setServiceVersion(String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    public ByteBuf getData() {
        return data;
    }

    public void setData(ByteBuf data) {
        this.data = data;
    }

    //FIXME ...you hua rm unused field
    public DubboFuture() {}

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isTwoWay() {
        return twoWay;
    }

    public void setTwoWay(boolean twoWay) {
        this.twoWay = twoWay;
    }

    public boolean isEvent() {
        return event;
    }

    public void setEvent(boolean event) {
        this.event = event;
    }

    public boolean isRequest() {
        return isRequest;
    }

    public void setRequest(boolean isRequest) {
        this.isRequest = isRequest;
    }

    public int getSerializationId() {
        return serializationId;
    }

    public void setSerializationId(int serializationId) {
        this.serializationId = serializationId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    private boolean isHeaderComplate;

    @Override
    public boolean read(NioSocketChannel channel, ByteBuf src) throws IOException {
        if (!isHeaderComplate) {
            int headerLen = DubboCodec.HEADER_LENGTH;
            if (src.remaining() < headerLen) {
                return false;
            }
            isHeaderComplate = true;
            src.skipBytes(2);
            byte byte2 = src.getByte();
            this.isRequest = (byte2 & 0b10000000) != 0;
            this.twoWay = (byte2 & 0b01000000) != 0;
            this.event = (byte2 & 0b00100000) != 0;
            this.serializationId = (byte2 & 0b00011111);
            this.status = src.getByte();
            this.id = src.getLong();
            int len = src.getInt();
            data = channel.allocator().allocate(len);
            if (src.remaining() < len) {
                return false;
            }
        }
        data.read(src);
        if (data.hasRemaining()) {
            return false;
        }
        data.flip();
        return true;
    }

    @Override
    public boolean isHeartbeat() {
        return isEvent();
    }

    @Override
    public boolean isPING() {
        return isHeartbeat() && isRequest();
    }

    @Override
    public boolean isPONG() {
        return isHeartbeat() && !isRequest();
    }

}
