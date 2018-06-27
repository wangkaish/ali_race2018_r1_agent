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

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;
import com.generallycloud.baseio.component.ChannelContext;
import com.generallycloud.baseio.component.NioSocketChannel;
import com.generallycloud.baseio.protocol.Future;
import com.generallycloud.baseio.protocol.ProtocolCodec;

/**
 * @author wangkai
 *
 */
public final class AgentCodec implements ProtocolCodec {

    private static final ByteBuf PING;
    private static final ByteBuf PONG;
    public static final int      PROTOCOL_HEADER = 4;
    public static final int      PROTOCOL_PING   = -1;
    public static final int      PROTOCOL_PONG   = -2;

    static {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.getHeap();
        PING = allocator.allocate(4);
        PONG = allocator.allocate(4);
        PING.putInt(PROTOCOL_PING);
        PONG.putInt(PROTOCOL_PONG);
        PING.flip();
        PONG.flip();
    }

    @Override
    public Future createPINGPacket(NioSocketChannel channel) {
        return new AgentFuture().setPing();
    }

    @Override
    public Future createPONGPacket(NioSocketChannel channel, Future ping) {
        return new AgentFuture().setPong();
    }

    @Override
    public Future decode(NioSocketChannel channel, ByteBuf buffer) throws IOException {
        return new AgentFuture();
    }

    @Override
    public ByteBuf encode(NioSocketChannel channel, Future future) throws IOException {
        ByteBufAllocator allocator = channel.allocator();
        if (future.isSilent()) {
            return future.isPing() ? PING.duplicate() : PONG.duplicate();
        }
        AgentFuture f = (AgentFuture) future;
        int writeSize = f.getWriteSize();
        if (writeSize == 0) {
            throw new IOException("null write buffer");
        }
        ByteBuf buf = allocator.allocate(writeSize + 4);
        buf.putInt(writeSize);
        buf.put(f.getWriteBuffer(), 0, writeSize);
        return buf.flip();
    }

    @Override
    public String getProtocolId() {
        return "AgentCodec";
    }

    @Override
    public void initialize(ChannelContext context) {}

}
