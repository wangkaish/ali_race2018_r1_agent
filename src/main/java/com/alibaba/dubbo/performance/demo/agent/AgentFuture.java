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
import com.generallycloud.baseio.component.NioSocketChannel;
import com.generallycloud.baseio.protocol.AbstractFuture;
import com.generallycloud.baseio.protocol.ProtocolException;

public final class AgentFuture extends AbstractFuture {

    private ByteBuf data;
    
    public ByteBuf getData() {
        return data;
    }

    public void setData(ByteBuf data) {
        this.data = data;
    }

    @Override
    public boolean read(NioSocketChannel channel, ByteBuf src) throws IOException {
        if (src.remaining() < 4) {
            return false;
        }
        int len = src.getInt();
        if (len < 1) {
            if (len == -3) {
                if (src.remaining() < 4) {
                    src.position(src.position() - 4);
                    return false;
                }
                setSilent();
                int channelId = src.getInt();
                AgentApp.agentClientMap.put(channelId, channel);
            }else if(len == -4){
                if (src.remaining() < 4) {
                    src.position(src.position() - 4);
                    return false;
                }
                setSilent();
                int channelId = src.getInt();
                AgentApp.agentClientMap.remove(channelId);
            }else{
                setHeartBeat(len);
            }
            return true;
        }
        if (src.remaining() < len) {
            src.position(src.position() - 4);
            return false;
        }
        data = channel.allocator().allocate(len);
        data.read(src);
        data.flip();
        return true;
    }

    private void setHeartBeat(int len) {
        if (len == AgentCodec.PROTOCOL_PING) {
            setPing();
        } else if (len == AgentCodec.PROTOCOL_PONG) {
            setPong();
        } else {
            throw new ProtocolException("illegal length:" + len);
        }
    }

}
