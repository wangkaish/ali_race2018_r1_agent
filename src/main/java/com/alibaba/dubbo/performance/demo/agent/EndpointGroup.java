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

import com.alibaba.dubbo.performance.demo.agent.AgentApp.CopyOnWriteArrayList;
import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.component.NioEventLoop;
import com.generallycloud.baseio.log.Logger;
import com.generallycloud.baseio.log.LoggerFactory;

/**
 * @author wangkai
 *
 */
public final class EndpointGroup {

//    private static final Logger  logger     = LoggerFactory.getLogger(EndpointGroup.class);

    private NioEventLoop         eventLoop;
    private String               interfaceName;
    private boolean              w_warmup   = true;
    private long                 w_firstReqTime;
    private int                  w_eIndex   = 0;
    private int                  w_reqIndex = 0;
    private CopyOnWriteArrayList endpoints  = new CopyOnWriteArrayList();

    public EndpointGroup(NioEventLoop eventLoop, String interfaceName) {
        this.eventLoop = eventLoop;
        this.interfaceName = interfaceName;
    }

    public void registEndpoint(Endpoint endpoint) {
        synchronized (endpoints) {
            if (endpoints.contains(endpoint)) {
                return;
            }
            endpoint.connect(eventLoop);
            endpoint.setGroup(this);
            endpoints.add(endpoint);
        }
    }

    public void deRegistHttpClient(int channelId) {
        synchronized (endpoints) {
            for (int i = 0; i < endpoints.size(); i++) {
                endpoints.get(i).deRegistHttpClient(channelId);
            }
        }
    }

    public synchronized void deRegistEndpoint(Endpoint endpoint) {
        synchronized (endpoints) {
            for (int i = 0; i < endpoints.size(); i++) {
                Endpoint target = endpoints.get(i);
                if (target.equals(endpoint)) {
                    CloseUtil.close(target);
                    endpoints.remove(i);
                    return;
                }
            }
        }
    }

    public int getMaxRt(Endpoint ignore) {
        Endpoint[] es = this.endpoints.eles;
        int size = es.length;
        int max = 0;
        for (int i = 0; i < size; i++) {
            Endpoint e = es[i];
            if (e == ignore) {
                continue;
            }
            if (max < e.getRt()) {
                max = e.getRt() + 1;
            }
        }
        return max;
    }

    private Endpoint findFreeEndPointWarmup() {
        Endpoint[] es = this.endpoints.eles;
        if (w_firstReqTime == 0) {
            w_firstReqTime = System.currentTimeMillis();
        }
        long now = System.currentTimeMillis();
        if (now - w_firstReqTime > 30 * 1000) {
            w_warmup = false;
            for(Endpoint e : es){
//                logger.info("e:{},rt:{}", e.getScaleType(), e.getRt());
            }
            return findFreeEndPoint();
        }
        if (w_reqIndex++ == 200) {
            w_reqIndex = 0;
            if (++w_eIndex == es.length) {
                w_eIndex = 0;
            }
        }
        return es[w_eIndex];
    }

    @SuppressWarnings("resource")
    public Endpoint findFreeEndPoint() {
        if (w_warmup) {
            return findFreeEndPointWarmup();
        }
        Endpoint[] es = this.endpoints.eles;
        Endpoint last = es[0];
        int size = es.length;
        for (int i = 1; i < size; i++) {
            Endpoint e = es[i];
            if (e.isFree() && e.getRt() <= last.getRt()) {
                last = e;
            }
        }
        return last;
    }

    public String getInterfaceName() {
        return interfaceName;
    }

    public CopyOnWriteArrayList getEndpoints() {
        return endpoints;
    }

    public NioEventLoop getEventLoop() {
        return eventLoop;
    }

    public static void registEndpoint(NioEventLoop eventLoop, Endpoint endpoint) {
        getEndpointGroup(eventLoop, endpoint.getServiceName()).registEndpoint(endpoint.clone());
    }

    public static void deRegistEndpoint(NioEventLoop eventLoop, Endpoint endpoint) {
        getEndpointGroup(eventLoop, endpoint.getServiceName()).deRegistEndpoint(endpoint);
    }

    public static EndpointGroup getEndpointGroup(NioEventLoop eventLoop, String interfaceName) {
        EndpointGroup group = (EndpointGroup) eventLoop.getAttribute(interfaceName);
        if (group == null) {
            synchronized (eventLoop) {
                group = (EndpointGroup) eventLoop.getAttribute(interfaceName);
                if (group != null) {
                    return group;
                }
                group = new EndpointGroup(eventLoop, interfaceName);
                eventLoop.setAttribute(interfaceName, group);
            }
        }
        return group;
    }

}
