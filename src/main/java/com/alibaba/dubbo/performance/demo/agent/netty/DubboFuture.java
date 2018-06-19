package com.alibaba.dubbo.performance.demo.agent.netty;

import java.util.Map;

import io.netty.buffer.ByteBuf;

public class DubboFuture {
    private long                id;
    private boolean             isRequest;
    private int                 serializationId = 6;
    private int                 status;
    private boolean             twoWay          = true;
    private boolean             event           = false;
    private String              dubboVersion    = "2.6.0";
    private String              serviceVersion  = "0.0.0";
    private Map<String, String> attachments;
    private ByteBuf            data;

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

}
