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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.generallycloud.baseio.common.ClassUtil;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoop;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.InternalThreadLocalMap;

/**
 * @author wangkai
 *
 */
public class NettyUtil {

    public static final int    mapIndex;
    private static final Field nioEventLoopThread;
    public static final int    sessionsIndex;

    static {
        Class<?> NioEventLoopClass = NioEventLoop.class;
        nioEventLoopThread = ClassUtil.getDeclaredFieldFC(NioEventLoopClass, "thread");
        ClassUtil.trySetAccessible(nioEventLoopThread);
        mapIndex = InternalThreadLocalMap.nextVariableIndex();
        sessionsIndex = InternalThreadLocalMap.nextVariableIndex();
    }

    private static FastThreadLocalThread get(EventLoop eventLoop) {
        try {
            return (FastThreadLocalThread) nioEventLoopThread.get(eventLoop);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Object getELV(FastThreadLocalThread thread, String key) {
        return getEventLoopMap(thread.threadLocalMap()).get(key);
    }

    public static Object getELV(EventLoop eventLoop, String key) {
        return getELV(get(eventLoop), key);
    }

    public static Object getELV(String key) {
        return getELV((FastThreadLocalThread) Thread.currentThread(), key);
    }

    public static Map<String, Object> getEventLoopMap() {
        return getEventLoopMap(InternalThreadLocalMap.get());
    }

    private static Map<String, Object> getEventLoopMap(InternalThreadLocalMap map) {
        Object res = map.indexedVariable(mapIndex);
        if (res == InternalThreadLocalMap.UNSET) {
            synchronized (map) {
                res = map.indexedVariable(mapIndex);
                if (res != InternalThreadLocalMap.UNSET) {
                    return (Map<String, Object>) res;
                }
                map.setIndexedVariable(mapIndex, new HashMap<>());
                res = map.indexedVariable(mapIndex);
            }
        }
        return (Map<String, Object>) res;
    }

    public static Map<String, Object> getEventLoopMap(EventLoop eventLoop) {
        return getEventLoopMap(get(eventLoop).threadLocalMap());
    }

    public static Channel getSession(int id){
        return getSessions(InternalThreadLocalMap.get()).get(id);
    }

    public static IntObjectHashMap<Channel> getSessions(InternalThreadLocalMap map){
        Object res =  map.indexedVariable(sessionsIndex);
        if (res == InternalThreadLocalMap.UNSET) {
            synchronized (map) {
                res = map.indexedVariable(sessionsIndex);
                if (res != InternalThreadLocalMap.UNSET) {
                    return (IntObjectHashMap<Channel>) res;
                }
                map.setIndexedVariable(sessionsIndex, new IntObjectHashMap<Channel>());
                res = map.indexedVariable(sessionsIndex);
            }
        }
        return (IntObjectHashMap<Channel>) res;
    }

    public static void putSession(int id,Channel channel){
        getSessions(InternalThreadLocalMap.get()).put(id, channel);
    }
    
    public static Channel removeSession(int id){
        return getSessions(InternalThreadLocalMap.get()).remove(id);
    }
    
    private static void setELV(FastThreadLocalThread thread, String key, Object value) {
        InternalThreadLocalMap map = thread.threadLocalMap();
        getEventLoopMap(map).put(key, value);
    }
    
    public static void setELV(EventLoop eventLoop, String key, Object value) {
        setELV(get(eventLoop), key, value);
    }
    
    public static void setELV(String key, Object value) {
        setELV((FastThreadLocalThread) Thread.currentThread(), key, value);
    }
    

}
