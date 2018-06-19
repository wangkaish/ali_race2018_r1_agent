package com.alibaba.dubbo.performance.demo.agent.netty;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.coreos.jetcd.watch.WatchResponse;
import com.generallycloud.baseio.common.ThreadUtil;
import com.generallycloud.baseio.common.ThrowableUtil;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.internal.InternalThreadLocalMap;

public class EtcdRegistry {
    
    private Logger logger = LoggerFactory.getLogger(getClass());
    // 该EtcdRegistry没有使用etcd的Watch机制来监听etcd的事件
    // 添加watch，在本地内存缓存地址列表，可减少网络调用的次数
    // 使用的是简单的随机负载均衡，如果provider性能不一致，随机策略会影响性能

    private final String rootPath = "dubbomesh";
    private Lease        lease;
    private KV           kv;
    private long         leaseId;
    private Watch        watch;

    public EtcdRegistry(String registryAddress) {
        try {
            init(registryAddress);
        } catch (Exception e) {
            logger.info(ThrowableUtil.stackTraceToString(e));
            try {
                init("http://127.0.0.1:2379");
            } catch (Exception e1) {
                logger.info(ThrowableUtil.stackTraceToString(e));
            }
        }
    }

    private void init(String address) throws InterruptedException, ExecutionException {
        Client client = Client.builder().endpoints(address).build();
        this.lease = client.getLeaseClient();
        this.kv = client.getKVClient();
        this.watch = client.getWatchClient();
        this.leaseId = lease.grant(30).get().getID();
        keepAlive();
        registerOrWatch();
    }

    private void registerOrWatch() {
        int port = Integer.valueOf(System.getProperty("server.port"));
        try {
            if (AgentApp.IS_PROVIDER) {
                // 如果是provider，去etcd注册服务
                register("com.alibaba.dubbo.performance.demo.provider.IHelloService", port);
            } else {
                String strKey = MessageFormat.format("/{0}/", rootPath);
                ByteSequence key = ByteSequence.fromString(strKey);
                Watcher watcher = this.watch.watch(key);
                ThreadUtil.exec(() -> {
                    for (;;) {
                        try {
                            WatchResponse res = watcher.listen();
                            List<WatchEvent> es = res.getEvents();
                            for (WatchEvent e : es) {
                                EventType eventType = e.getEventType();
                                KeyValue kv = e.getKeyValue();
                                modifyEndpoint(kv, eventType == EventType.PUT);
                            }
                        } catch (Throwable e) {
                            logger.info(ThrowableUtil.stackTraceToString(e));
                        }
                    }
                }, "etcd-service-watcher");
                ThreadUtil.sleep(8);
                fetchBase();
            }
        } catch (Exception e) {
            logger.info(ThrowableUtil.stackTraceToString(e));
        }
    }

    private void fetchBase() throws InterruptedException, ExecutionException {
        String strKey = MessageFormat.format("/{0}/", rootPath);
        ByteSequence key = ByteSequence.fromString(strKey);
        GetResponse response = kv.get(key, GetOption.newBuilder().withPrefix(key).build()).get();
        for (KeyValue kv : response.getKvs()) {
            modifyEndpoint(kv, true);
        }
    }

    private void modifyEndpoint(KeyValue kv, boolean add) {
        EventLoopGroup group = AgentApp.workerGroup;
        logger.info("key:{},add:{}", kv.getKey().toString(), Boolean.toString(add));
        Endpoint endpoint = buildEndpoint(kv);
        logger.info("build endpoint:{}",endpoint);
        group.forEach((eventLoop) -> {
            final CountDownLatch c = new CountDownLatch(1);
            eventLoop.submit(() -> {
                InternalThreadLocalMap.get();
                c.countDown();
            });
            try {
                c.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            if (add) {
                EndpointGroup.registEndpoint((EventLoop)eventLoop, endpoint.clone());
            } else {
                EndpointGroup.deRegistEndpoint((EventLoop)eventLoop, endpoint.clone());
            }
        });
    }

    // 向ETCD中注册服务
    public void register(String serviceName, int port) throws Exception {
        String host;
        if (AgentApp.RUN_MODE_LOCAL) {
            host = "127.0.0.1";
        } else {
            host = AgentApp.SERVER_HOST;
        }
        String strKey = MessageFormat.format("/{0}/{1}/{2}:{3}:{4}:{5}", 
                rootPath, 
                serviceName, 
                host,
                String.valueOf(port), 
                String.valueOf(AgentApp.PROVIDER_SCALE_TYPE),
                String.valueOf(AgentApp.THREADS));
        ByteSequence key = ByteSequence.fromString(strKey);
        ByteSequence val = ByteSequence.fromString(""); // 目前只需要创建这个key,对应的value暂不使用,先留空
        kv.put(key, val, PutOption.newBuilder().withLeaseId(leaseId).build()).get();
        logger.info("Register a new service at:" + strKey);
    }

    // 发送心跳到ETCD,表明该host是活着的
    public void keepAlive() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                Lease.KeepAliveListener listener = lease.keepAlive(leaseId);
                listener.listen();
                logger.info("KeepAlive lease:{}; Hex format:{}", leaseId,
                        Long.toHexString(leaseId));
            } catch (Exception e) {
                logger.info(ThrowableUtil.stackTraceToString(e));
            }
        });
    }

    private Endpoint buildEndpoint(KeyValue kv) {
        String s = kv.getKey().toStringUtf8();
        String[] arr = s.split("/");
        String serviceName = arr[2];
        String[] endpoints = arr[3].split(":");
        String host = endpoints[0];
        int port = Integer.parseInt(endpoints[1]);
        int scaleType = Integer.parseInt(endpoints[2]);
        int threads = Integer.parseInt(endpoints[3]);
        int maxTask = (threads / AgentApp.THREADS);
        return new Endpoint(serviceName, host, port, scaleType, maxTask);
    }

}
