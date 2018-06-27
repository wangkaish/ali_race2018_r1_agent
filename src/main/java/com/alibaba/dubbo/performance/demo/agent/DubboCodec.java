/*
 * Copyright 2015-2017 GenerallyCloud.com
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.performance.demo.agent;

import java.io.IOException;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.component.ChannelContext;
import com.generallycloud.baseio.component.NioSocketChannel;
import com.generallycloud.baseio.protocol.Future;
import com.generallycloud.baseio.protocol.ProtocolCodec;

/**
 * @author wangkai
 *
 */
public final class DubboCodec implements ProtocolCodec {

    public static final byte  FLAG_EVENT    = (byte) 0x20;
    // message flag.
    public static final byte  FLAG_REQUEST  = (byte) 0x80;
    public static final byte  FLAG_TWOWAY   = (byte) 0x40;
    // header length.
    public static final int   HEADER_LENGTH = 16;
    public static final byte  LF            = (byte) '\n';
    // magic header.
    public static final short MAGIC         = (short) 0xdabb;
    public static final byte  QT            = (byte) '\"';

    static final byte [] NULL = "null".getBytes();
    
    @Override
    public Future createPINGPacket(NioSocketChannel channel) {
        DubboFuture f = new DubboFuture();
        f.setEvent(true);
        f.setRequest(true);
        f.setTwoWay(true);
        return f;
    }

    @Override
    public Future createPONGPacket(NioSocketChannel channel, Future ping) {
        DubboFuture f = (DubboFuture) ping;
        f.setEvent(true);
        f.setRequest(false);
        f.setTwoWay(false);
        return f;
    }

    @Override
    public Future decode(NioSocketChannel channel, ByteBuf buffer) throws IOException {
        return new DubboFuture();
    }

    @Override
    public ByteBuf encode(NioSocketChannel channel, Future future) throws IOException {
        DubboFuture f = (DubboFuture) future;
        if (f.isEvent()) {
            return encodeEvent(channel, f);
        }
        ByteBuf data = f.getData();
        int id = data.getInt();
        int snbsLen = data.getByte();
        int mnbsLen = data.getByte();
        int mptbsLen = data.getByte();
        int mabsLen = data.getInt();
        byte[] dvbs = f.getDubboVersion().getBytes();
        byte[] snbs = null;
        byte[] svbs = f.getServiceVersion().getBytes();
        byte[] mnbs = null;
        byte[] mptbs = null;
        byte[] mabs = null;
        byte[] atbs = parseAttachments(f.getAttachments());
        int len = HEADER_LENGTH + 6 * 3 + 1 + dvbs.length + snbsLen + svbs.length + mnbsLen
                + mptbsLen + mabsLen + atbs.length;
        ByteBuf buf = channel.allocator().allocate(len); //FIXME ..... len
        buf.putShort(MAGIC);
        byte byte2 = (byte) (FLAG_REQUEST | 6);
        if (f.isTwoWay())
            byte2 |= FLAG_TWOWAY;
        if (f.isEvent())
            byte2 |= FLAG_EVENT;
        buf.putByte(byte2);
        buf.putByte((byte) 0);
        buf.putLong(id);
        buf.putInt(len - HEADER_LENGTH);
        //    * Dubbo version
        //    * Service name
        //    * Service version
        //    * Method name
        //    * Method parameter types
        //    * Method arguments
        //    * Attachments
        //FIXME ..youhua
        buf.putByte(QT);
        buf.put(dvbs);
        buf.putByte(QT);
        buf.putByte(LF);
        buf.putByte(QT);
        buf.read(data, snbsLen);
        buf.putByte(QT);
        buf.putByte(LF);
        buf.putByte(QT);
        buf.put(svbs);
        buf.putByte(QT);
        buf.putByte(LF);
        buf.putByte(QT);
        buf.read(data, mnbsLen);
        buf.putByte(QT);
        buf.putByte(LF);
        buf.putByte(QT);
        buf.read(data, mptbsLen);
        buf.putByte(QT);
        buf.putByte(LF);
        buf.putByte(QT);
        buf.read(data, mabsLen);
        buf.putByte(QT);
        buf.putByte(LF);
        buf.put(atbs);
        buf.putByte(LF);
        data.release(data.getReleaseVersion());
        return buf.flip();
    }

    public ByteBuf encodeEvent(NioSocketChannel channel, DubboFuture f) throws IOException {
        int len = HEADER_LENGTH + 5;
        ByteBuf buf = channel.allocator().allocate(len); //FIXME ..... len
        buf.putShort(MAGIC);
        byte byte2 = (byte) (FLAG_REQUEST | 6);
        if (f.isTwoWay())
            byte2 |= FLAG_TWOWAY;
        if (f.isEvent())
            byte2 |= FLAG_EVENT;
        buf.putByte(byte2);
        buf.putByte((byte) 0);
        buf.putLong(f.getId());
        buf.putInt(5);
        buf.put(NULL);
        buf.putByte(LF);
        return buf.flip();
    }

    @Override
    public String getProtocolId() {
        return "dubbo";
    }

    @Override
    public void initialize(ChannelContext context) {}

    private byte[] parseAttachments(Map<String, String> attachments) {
        if (attachments == null) {
            return NULL;
        }
        return JSONObject.toJSONString(attachments).getBytes();
    }

}
