package com.alibaba.dubbo.performance.demo.agent;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.StandardSocketOptions;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufUtil;
import com.generallycloud.baseio.codec.http11.ServerHttpCodec;
import com.generallycloud.baseio.codec.http11.ServerHttpFuture;
import com.generallycloud.baseio.collection.IntObjectHashMap;
import com.generallycloud.baseio.common.Encoding;
import com.generallycloud.baseio.common.StringUtil;
import com.generallycloud.baseio.component.ChannelAcceptor;
import com.generallycloud.baseio.component.ChannelAliveIdleEventListener;
import com.generallycloud.baseio.component.ChannelContext;
import com.generallycloud.baseio.component.ChannelEventListenerAdapter;
import com.generallycloud.baseio.component.IoEventHandleAdaptor;
import com.generallycloud.baseio.component.LoggerChannelOpenListener;
import com.generallycloud.baseio.component.NioEventLoop;
import com.generallycloud.baseio.component.NioEventLoopGroup;
import com.generallycloud.baseio.component.NioEventLoopTask;
import com.generallycloud.baseio.component.NioSocketChannel;
import com.generallycloud.baseio.component.ReConnector;
import com.generallycloud.baseio.concurrent.EventLoop;
import com.generallycloud.baseio.concurrent.EventLoopListener;
import com.generallycloud.baseio.configuration.Configuration;
import com.generallycloud.baseio.log.Logger;
import com.generallycloud.baseio.log.LoggerFactory;
import com.generallycloud.baseio.protocol.Future;

public class AgentApp {
    // agent会作为sidecar，部署在每一个Provider和Consumer机器上
    // 在Provider端启动agent时，添加JVM参数-Dtype=provider -Dserver.port=30000 -Ddubbo.protocol.port=20889
    // 在Consumer端启动agent时，添加JVM参数-Dtype=consumer -Dserver.port=20000
    // 添加日志保存目录: -Dlogs.dir=/path/to/your/logs/dir。请安装自己的环境来设置日志目录。

    public static final int                                CORE_SIZE;
    public static final boolean                            BATCH_FLUSH            = false;
    public static final int                                WRITE_BUFFER           = 16;
    public static final boolean                            ENABLE_MEM_POOL        = true;
    public static final boolean                            ENABLE_MEM_POOL_DIRECT = true;
    public static final String                             NEW_LINE;
    public static final int                                NEW_LINE_LEN;
    public static final boolean                            IS_PROVIDER;
    public static final String                             SERVER_HOST;
    public static final int                                SERVER_PORT;
    public static final int                                THREADS;
    public static final int                                DUBBO_PROTOCOL_PORT;
    public static final int                                PROVIDER_SCALE_TYPE;
    public static final boolean                            RUN_MODE_LOCAL;
    public static final IntObjectHashMap<NioSocketChannel> agentClientMap         = new IntObjectHashMap<>();
    public static final NioEventLoopGroup                  consumerAgentGroup     = new NioEventLoopGroup();
    private static final Map<String, byte[]>               staticParamBytes       = new ConcurrentHashMap<>();
//    private static final Logger                            logger                 = LoggerFactory
//            .getLogger(AgentApp.class);
    private static EtcdRegistry                            registry;
    private static NioSocketChannel                        providerClient         = null;
    private static final int                               CAReqBufVarIndex       = NioEventLoop
            .nextIndexedVariablesIndex();

    static {
        LoggerFactory.setEnableDebug(false);
        LoggerFactory.setEnableInfo(false);
        String type = System.getProperty("type"); // 获取type参数
        String runModeLocal = System.getProperty("run_mode_local");
        SERVER_HOST = System.getProperty("server.host");
        SERVER_PORT = Integer.parseInt(System.getProperty("server.port"));
        RUN_MODE_LOCAL = !StringUtil.isNullOrBlank(runModeLocal);
        CORE_SIZE = Runtime.getRuntime().availableProcessors();
        NEW_LINE = System.getProperty("line.separator");
        NEW_LINE_LEN = NEW_LINE.length();
        THREADS = Integer.parseInt(System.getProperty("threads"));
        if ("consumer".equals(type)) {
            IS_PROVIDER = false;
            DUBBO_PROTOCOL_PORT = 0;
            PROVIDER_SCALE_TYPE = 0;
            consumerAgentGroup.setEventLoopListener(new EventLoopListener() {

                @Override
                public void onStop(EventLoop eventLoop) {
                    NioEventLoop e = (NioEventLoop) eventLoop;
                    ByteBuffer buffer = (ByteBuffer) e.getIndexedVariable(CAReqBufVarIndex);
                    ByteBufUtil.release(buffer);
                }

                @Override
                public void onStartup(EventLoop eventLoop) {
                    NioEventLoop e = (NioEventLoop) eventLoop;
                    e.setIndexedVariable(CAReqBufVarIndex, ByteBuffer.allocateDirect(1024 * 4));
                }
            });
        } else if ("provider".equals(type)) {
            IS_PROVIDER = true;
            DUBBO_PROTOCOL_PORT = Integer.parseInt(System.getProperty("dubbo.protocol.port"));
            PROVIDER_SCALE_TYPE = Integer.parseInt(System.getProperty("scale.type"));
        } else {
            throw new Error("Environment variable type is needed to set to provider or consumer.");
        }
        initParamDecodes();
    }

    private static void initParamDecodes() {
        getStaticParamBytes("Ljava%2Flang%2FString%3B");
        getStaticParamBytes("com.alibaba.dubbo.performance.demo.provider.IHelloService");
        getStaticParamBytes("hash");
    }

    private static byte[] getStaticParamBytes(String key) {
        byte[] res = staticParamBytes.get(key);
        if (res == null) {
            synchronized (staticParamBytes) {
                res = staticParamBytes.get(key);
                if (res != null) {
                    return res;
                }
                try {
                    res = URLDecoder.decode(key, "UTF-8").getBytes();
                    staticParamBytes.put(key, res);
                } catch (UnsupportedEncodingException e) {}
            }
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        if (IS_PROVIDER) {
            initProviderAgent();
        } else {
            initConsumerAgent();
        }
    }

    private static void initConsumerAgent() throws IOException {
        NioEventLoopGroup group = consumerAgentGroup;
        Configuration cfg = new Configuration(SERVER_PORT);
        group.setEnableMemoryPool(ENABLE_MEM_POOL);
        group.setEnableMemoryPoolDirect(ENABLE_MEM_POOL_DIRECT);
        group.setMemoryPoolRate(16);
        group.setMemoryPoolUnit(512);
        group.setEventLoopSize(THREADS);
        group.setWriteBuffers(WRITE_BUFFER);
        group.setSharable(true);
        ChannelContext context = new ChannelContext(cfg);
        ChannelAcceptor acceptor = new ChannelAcceptor(context, group);
        context.setIoEventHandle(new IoEventHandleAdaptor() {

            @Override
            public final void accept(NioSocketChannel channel, Future future) throws Exception {
                ServerHttpFuture f = (ServerHttpFuture) future;
                // logger.info("req params:{}", params);
                String interfaceName = f.getRequestParam("interface");
                String method = f.getRequestParam("method");
                String parameterTypesString = f.getRequestParam("parameterTypesString");
                String parameter = f.getRequestParam("parameter");
                if (parameter == null) {
                    parameter = "";
                }
                int channelId = channel.getChannelId();
                NioEventLoop eventLoop = channel.getEventLoop();
                EndpointGroup endpointGroup = (EndpointGroup) eventLoop.getAttribute(interfaceName);
                Endpoint endpoint = endpointGroup.findFreeEndPoint();
                //                if (!endpoint.isFree()) {
                //                    logger.info("no free endpoint");
                //                    f.setStatus(HttpStatus.C503);
                //                    channel.flush(f);
                //                    return;
                //                }
                endpoint.registHttpClient(channelId);
                ByteBuffer CAReqBuf = (ByteBuffer) eventLoop.getIndexedVariable(CAReqBufVarIndex);
                CAReqBuf.clear();
                byte[] interfaceNameBytes = getStaticParamBytes(interfaceName);
                byte[] methodBytes = getStaticParamBytes(method);
                byte[] parameterTypesStringBytes = getStaticParamBytes(parameterTypesString);
                CharsetEncoder encoder = eventLoop.getCharsetEncoder(Encoding.UTF8);
                encoder.reset();
                encoder.encode(CharBuffer.wrap(parameter), CAReqBuf, true);
                CAReqBuf.flip();
                int snbsLen = interfaceNameBytes.length;
                int mnbsLen = methodBytes.length;
                int mptbsLen = parameterTypesStringBytes.length;
                int mabsLen = CAReqBuf.limit();
                int len = 4 + 4 + 1 + 1 + 1 + 4 + snbsLen + mnbsLen + mptbsLen + mabsLen;
                ByteBuf buf = endpoint.allocator().allocate(len);
                buf.putInt(len - 4);
                buf.putInt(channelId);
                buf.putByte((byte) snbsLen);
                buf.putByte((byte) mnbsLen);
                buf.putByte((byte) mptbsLen);
                buf.putInt(CAReqBuf.limit());
                buf.put(interfaceNameBytes);
                buf.put(methodBytes);
                buf.put(parameterTypesStringBytes);
                buf.read(CAReqBuf);
                f.setByteBuf(buf.flip());
                endpoint.flushChannelFuture(channelId, f);
            }
        });
        context.addChannelEventListener(new ChannelEventListenerAdapter() {

            @Override
            public void channelClosed(NioSocketChannel channel) {
                for (Object o : channel.attributes().values()) {
                    if (o instanceof EndpointGroup) {
                        ((EndpointGroup) o).deRegistHttpClient(channel.getChannelId());
                    }
                }
            }
        });
        context.addChannelEventListener(new SetTCP_NODELAYSEListener());
        context.setProtocolCodec(new ServerHttpCodec());
        acceptor.bind();
        registry = new EtcdRegistry(System.getProperty("etcd.url"));
    }

    private static void initProviderAgent() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        Configuration cfg = new Configuration(SERVER_PORT);
        group.setEnableMemoryPool(ENABLE_MEM_POOL);
        group.setEnableMemoryPoolDirect(ENABLE_MEM_POOL_DIRECT);
        group.setMemoryPoolRate(16);
        group.setWriteBuffers(WRITE_BUFFER);
        group.setSharable(true);
        ChannelContext context = new ChannelContext(cfg);
        ChannelAcceptor acceptor = new ChannelAcceptor(context, group);
        context.addChannelEventListener(new LoggerChannelOpenListener());
        context.addChannelEventListener(new SetTCP_NODELAYSEListener());
        if (!RUN_MODE_LOCAL) {
            context.addChannelIdleEventListener(new ChannelAliveIdleEventListener());
        }
        context.addChannelEventListener(new ChannelEventListenerAdapter() {
            @Override
            public void channelOpened(NioSocketChannel channel) throws Exception {
                channel.setAttribute("BatchFlushTask", new BatchFlushTask());
            }
        });
        final DubboFuture df = new DubboFuture();
        context.setIoEventHandle(new IoEventHandleAdaptor() {
            
            private boolean needAddTask = true;
            
            private final List<Future> futures = new ArrayList<>(200);
            
            @Override
            public final void accept(NioSocketChannel channel, Future future) throws Exception {
                AgentFuture f = (AgentFuture) future;
                df.setData(f.getData());
                providerClient.getProtocolCodec().encode(channel, df);
                f.setByteBuf(df.getByteBuf());
                if (BATCH_FLUSH) {
                    futures.add(future);
                    if (needAddTask) {
                        needAddTask = false;
                        providerClient.getEventLoop().dispatchAfterLoop(new NioEventLoopTask() {
                            
                            @Override
                            public void fireEvent(NioEventLoop eventLoop) throws IOException {
                                providerClient.flushFutures(futures);
                                futures.clear();
                                needAddTask = true;
                            }
                        });
                    }
                }else{
                    providerClient.flushFuture(f);
                }
            }
        });
        context.setProtocolCodec(new AgentCodec());
        acceptor.bind();
        registry = new EtcdRegistry(System.getProperty("etcd.url"));
        Configuration dcfg;
        if (RUN_MODE_LOCAL) {
            dcfg = new Configuration(DUBBO_PROTOCOL_PORT);
        } else {
            dcfg = new Configuration(SERVER_HOST, DUBBO_PROTOCOL_PORT);
        }
        ChannelContext dcontext = new ChannelContext(dcfg);
        ReConnector providerConnector = new ReConnector(dcontext, group);
        dcontext.setIoEventHandle(new IoEventHandleAdaptor() {
            @Override
            public final void accept(NioSocketChannel channel, Future future) throws Exception {
                DubboFuture df = (DubboFuture) future;
                ByteBuf hashbs = df.getData();
                hashbs.skipBytes(NEW_LINE_LEN + 1);
                hashbs.limit(hashbs.limit() - NEW_LINE_LEN);
                int id = (int) df.getId();
                int len = 4 + hashbs.remaining();
                final NioSocketChannel agentClient = agentClientMap.get(id);
                if (agentClient == null) {
//                    logger.info("channel closed id:{}", String.valueOf(id));
                    return;
                }
                ByteBuf buf = agentClient.allocator().allocate(len + 4);
                buf.putInt(len);
                buf.putInt(id);
                buf.read(hashbs);
                hashbs.release(hashbs.getReleaseVersion());
                df.setByteBuf(buf.flip());
                if (BATCH_FLUSH) {
                    final BatchFlushTask task = (BatchFlushTask) agentClient
                            .getAttribute("BatchFlushTask");
                    task.futures.add(df);
                    if (task.needAddTask) {
                        task.needAddTask = false;
                        agentClient.getEventLoop().dispatchAfterLoop(new NioEventLoopTask() {

                            @Override
                            public void fireEvent(NioEventLoop eventLoop) throws IOException {
                                agentClient.flushFutures(task.futures);
                                task.futures.clear();
                                task.needAddTask = true;
                            }
                        });
                    }
                } else {
                    agentClient.flushFuture(df);
                }
            }
        });

        dcontext.addChannelEventListener(new ChannelEventListenerAdapter() {

            @Override
            public void channelOpened(NioSocketChannel channel) throws Exception {
                providerClient = channel;
            }
        });
        dcontext.addChannelEventListener(new SetTCP_NODELAYSEListener());
        dcontext.addChannelEventListener(new LoggerChannelOpenListener());
        dcontext.setProtocolCodec(new DubboCodec());
        providerConnector.setRetryTime(1000);
        providerConnector.connect();
    }

    public static void setSystemPropertiesIfNull(String key, String value) {
        String pro = System.getProperty(key);
        if (pro == null || pro.length() == 0) {
            System.setProperty(key, value);
        }
    }

    public static class SetTCP_NODELAYSEListener extends ChannelEventListenerAdapter {
        @Override
        public void channelOpened(NioSocketChannel channel) throws Exception {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            int SO_SNDBUF = channel.getOption(StandardSocketOptions.SO_SNDBUF);
            int SO_RCVBUF = channel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (SO_SNDBUF < 1024 * 256) {
                channel.setOption(StandardSocketOptions.SO_SNDBUF, 1024 * 256);
            }
            if (SO_RCVBUF < 1024 * 256) {
                channel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 256);
            }
        }
    }

    public static class BatchFlushTask {
        boolean      needAddTask = true;
        List<Future> futures     = new ArrayList<>(200);
    }

    public static class CopyOnWriteArrayList {

        ReentrantLock lock = new ReentrantLock();

        Endpoint[]    eles = new Endpoint[0];

        public void add(Endpoint v) {
            ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int len = eles.length;
                Endpoint[] newElements = Arrays.copyOf(eles, len + 1);
                newElements[len] = v;
                Arrays.sort(newElements);
                eles = newElements;
            } finally {
                lock.unlock();
            }
        }

        public Endpoint[] getSnapshot() {
            return this.eles;
        }

        public Endpoint remove(Endpoint v) {
            ReentrantLock lock = this.lock;
            lock.lock();
            try {
                Endpoint[] eles = this.eles;
                int index = -1;
                for (int i = 0; i < eles.length; i++) {
                    if (eles[i].equals(v)) {
                        index = i;
                        break;
                    }
                }
                return remove(index);
            } finally {
                lock.unlock();
            }
        }

        public boolean contains(Endpoint v) {
            Endpoint[] eles = this.eles;
            for (int i = 0; i < eles.length; i++) {
                if (eles[i].equals(v)) {
                    return true;
                }
            }
            return false;
        }

        public int size() {
            return eles.length;
        }

        public Endpoint get(int i) {
            return eles[i];
        }

        public Endpoint remove(int index) {
            ReentrantLock lock = this.lock;
            lock.lock();
            try {
                if (index > -1) {
                    Endpoint remove = (Endpoint) eles[index];
                    int len = eles.length;
                    Endpoint[] newElements = new Endpoint[len - 1];
                    if (index == 0) {
                        System.arraycopy(eles, 1, newElements, 0, len - 1);
                        this.eles = newElements;
                    } else if (index == len - 1) {
                        System.arraycopy(eles, 0, newElements, 0, len - 1);
                        this.eles = newElements;
                    } else {
                        System.arraycopy(eles, 0, newElements, 0, index);
                        System.arraycopy(eles, index + 1, newElements, index, len - index - 1);
                        this.eles = newElements;
                    }
                    return remove;
                }
                return null;
            } finally {
                lock.unlock();
            }
        }
    }

}
