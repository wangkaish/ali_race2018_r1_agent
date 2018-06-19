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
package com.alibaba.dubbo.performance.demo.agent.netty;

import java.io.IOException;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * @author wangkai
 *
 */
public class DubboEncoder extends ChannelOutboundHandlerAdapter {

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

    public void writeEvent(ChannelHandlerContext ctx, DubboFuture f, ChannelPromise promise) throws IOException {
        int len = HEADER_LENGTH + 5;
        ByteBuf buf = ctx.alloc().directBuffer(len);  //FIXME ..... len
        buf.writeShort(MAGIC);
        byte byte2 = (byte) (FLAG_REQUEST | 6);
        if (f.isTwoWay())
            byte2 |= FLAG_TWOWAY;
        if (f.isEvent())
            byte2 |= FLAG_EVENT;
        buf.writeByte(byte2);
        buf.writeByte((byte) 0);
        buf.writeLong(f.getId());
        buf.writeInt(5);
        buf.writeBytes(NULL);
        buf.writeByte(LF);
        ctx.write(buf, promise);
    }
    
    public static long byte2Long(byte[] bytes, int offset) {
        long v7 = (long) (bytes[offset + 0] & 0xff) << 8 * 7;
        long v6 = (long) (bytes[offset + 1] & 0xff) << 8 * 6;
        long v5 = (long) (bytes[offset + 2] & 0xff) << 8 * 5;
        long v4 = (long) (bytes[offset + 3] & 0xff) << 8 * 4;
        long v3 = (long) (bytes[offset + 4] & 0xff) << 8 * 3;
        long v2 = (long) (bytes[offset + 5] & 0xff) << 8 * 2;
        long v1 = (long) (bytes[offset + 6] & 0xff) << 8 * 1;
        long v0 = bytes[offset + 7] & 0xff;
        return (v0 | v1 | v2 | v3 | v4 | v5 | v6 | v7);
    }
    

    public static int byte2UnsignedShort(byte[] bytes, int offset) {
        int v1 = (bytes[offset + 0] & 0xff) << 8 * 1;
        int v0 = (bytes[offset + 1] & 0xff);
        return v0 | v1;
    }
    
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        DubboFuture f = (DubboFuture) msg;
        if (f.isEvent()) {
            writeEvent(ctx, f, promise);
            return;
        }
        ByteBuf data = f.getData();
        long id = data.readInt();
        int snbsLen = data.readByte();
        int mnbsLen = data.readByte();
        int mptbsLen = data.readByte();
        int mabsLen = data.readShort();
        byte[] dvbs = f.getDubboVersion().getBytes();
        byte[] snbs = null;
        byte[] svbs = f.getServiceVersion().getBytes();
        byte[] mnbs = null;
        byte[] mptbs = null;
        byte[] mabs = null;
        byte[] atbs = parseAttachments(f.getAttachments());
        int len = HEADER_LENGTH + 6 * 3 + 1 + dvbs.length + snbsLen + svbs.length + mnbsLen
                + mptbsLen + mabsLen + atbs.length;
        ByteBuf buf = ctx.alloc().directBuffer(len); //FIXME ..... len
        buf.writeShort(MAGIC);
        byte byte2 = (byte) (FLAG_REQUEST | 6);
        if (f.isTwoWay())
            byte2 |= FLAG_TWOWAY;
        if (f.isEvent())
            byte2 |= FLAG_EVENT;
        buf.writeByte(byte2);
        buf.writeByte((byte) 0);
        buf.writeLong(id);
        buf.writeInt(len - HEADER_LENGTH);
        //    * Dubbo version
        //    * Service name
        //    * Service version
        //    * Method name
        //    * Method parameter types
        //    * Method arguments
        //    * Attachments
        //FIXME ..youhua
        buf.writeByte(QT);
        buf.writeBytes(dvbs);
        buf.writeByte(QT);
        buf.writeByte(LF);
        buf.writeByte(QT);
        buf.writeBytes(data,snbsLen);
        buf.writeByte(QT);
        buf.writeByte(LF);
        buf.writeByte(QT);
        buf.writeBytes(svbs);
        buf.writeByte(QT);
        buf.writeByte(LF);
        buf.writeByte(QT);
        buf.writeBytes(data,mnbsLen);
        buf.writeByte(QT);
        buf.writeByte(LF);
        buf.writeByte(QT);
        buf.writeBytes(data,mptbsLen);
        buf.writeByte(QT);
        buf.writeByte(LF);
        buf.writeByte(QT);
        buf.writeBytes(data,mabsLen);
        buf.writeByte(QT);
        buf.writeByte(LF);
        buf.writeBytes(atbs);
        buf.writeByte(LF);
        ctx.writeAndFlush(buf, promise);
    }
    
    private byte[] parseAttachments(Map<String, String> attachments) {
        if (attachments == null) {
            return NULL;
        }
        return JSONObject.toJSONString(attachments).getBytes();
    }

}
