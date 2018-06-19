package com.alibaba.dubbo.performance.demo.agent.netty;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.generallycloud.baseio.common.Encoding;
import com.generallycloud.baseio.common.StringUtil;
import com.generallycloud.baseio.log.Logger;
import com.generallycloud.baseio.log.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;

public class AgentApp {
    // agent会作为sidecar，部署在每一个Provider和Consumer机器上
    // 在Provider端启动agent时，添加JVM参数-Dtype=provider -Dserver.port=30000 -Ddubbo.protocol.port=20889
    // 在Consumer端启动agent时，添加JVM参数-Dtype=consumer -Dserver.port=20000
    // 添加日志保存目录: -Dlogs.dir=/path/to/your/logs/dir。请安装自己的环境来设置日志目录。

    public static final int                       CORE_SIZE;
    public static final boolean                   ENABLE_MEM_POOL        = true;
    public static final boolean                   ENABLE_MEM_POOL_DIRECT = true;
    public static final String                    NEW_LINE;
    public static final int                       NEW_LINE_LEN;
    public static final boolean                   IS_PROVIDER;
    public static final String                    SERVER_HOST;
    public static final int                       SERVER_PORT;
    public static final int                       THREADS;
    public static final int                       DUBBO_PROTOCOL_PORT;
    public static final int                       PROVIDER_SCALE_TYPE;
    public static final boolean                   RUN_MODE_LOCAL;
    public static final IntObjectHashMap<Channel> agentClientMap         = new IntObjectHashMap<>();
    private static final Map<String, byte[]>      staticParamBytes       = new ConcurrentHashMap<>();
    private static final Logger                   logger                 = LoggerFactory
            .getLogger(AgentApp.class);
    private static EtcdRegistry                   registry;
    private static Channel                        providerClient         = null;
    public static EventLoopGroup                  boosGroup;
    public static EventLoopGroup                  workerGroup;
    public static Class<? extends ServerChannel>  serverSocketChannel;
    public static Class<? extends SocketChannel>  socketChannel;
    public static final AttributeKey<Integer>     CHANNEL_ID_KEY         = AttributeKey
            .newInstance("CHANNEL_ID_KEY");
    public static final AtomicInteger             CHANNEL_ID_SEQ         = new AtomicInteger(1);

    static {
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
        } else if ("provider".equals(type)) {
            IS_PROVIDER = true;
            DUBBO_PROTOCOL_PORT = Integer.parseInt(System.getProperty("dubbo.protocol.port"));
            PROVIDER_SCALE_TYPE = Integer.parseInt(System.getProperty("scale.type"));
        } else {
            throw new Error("Environment variable type is needed to set to provider or consumer.");
        }
        int eventLoopSize = THREADS;
        if (IS_PROVIDER) {
            eventLoopSize = 1;
        }
        if (RUN_MODE_LOCAL) {
            boosGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup(eventLoopSize);
            serverSocketChannel = NioServerSocketChannel.class;
            socketChannel = NioSocketChannel.class;
        } else {
            boosGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup(eventLoopSize);
            serverSocketChannel = EpollServerSocketChannel.class;
            socketChannel = EpollSocketChannel.class;
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
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workerGroup).channel(serverSocketChannel)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_BACKLOG, 2048)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .childOption(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                .childOption(ChannelOption.ALLOW_HALF_CLOSURE, Boolean.FALSE)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("log-channel", new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                int channelId = CHANNEL_ID_SEQ.getAndIncrement();
                                Attribute<Integer> chIdAttr = ctx.channel().attr(CHANNEL_ID_KEY);
                                chIdAttr.set(channelId);
                                NettyUtil.putSession(channelId, ctx.channel());
                                ctx.fireChannelActive();
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx)
                                    throws Exception {
                                Attribute<Integer> chIdAttr = ctx.channel().attr(CHANNEL_ID_KEY);
                                NettyUtil.removeSession(chIdAttr.get());
                                ctx.fireChannelInactive();
                                //                                for (Object o : ctx.channel().getAttributes().values()) {
                                //                                    if (o instanceof EndpointGroup) {
                                //                                        ((EndpointGroup) o).deRegistHttpClient(session.getSessionId());
                                //                                    }
                                //                                }
                            }
                        }).addLast("http-decoder", new HttpRequestDecoder())
                                .addLast("http-aggregator", new HttpObjectAggregator(1024 * 64))
                                .addLast("serverHandler", new ConsumerAgentHandler());
                    }
                });
        if(Epoll.isAvailable()){
            serverBootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED)
                    .option(EpollChannelOption.TCP_QUICKACK, java.lang.Boolean.TRUE);
        }
        ChannelFuture future = serverBootstrap.bind(new InetSocketAddress(SERVER_PORT));
        future.syncUninterruptibly();
        logger.info("agent-consumer-server:{}", future.channel().toString());
        registry = new EtcdRegistry(System.getProperty("etcd.url"));
    }

    private static void initProviderAgent() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workerGroup).channel(serverSocketChannel)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_BACKLOG, 2048)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .childOption(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                .childOption(ChannelOption.ALLOW_HALF_CLOSURE, Boolean.FALSE)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("agent-decoder", new AgentDecoder()).addLast(
                                "serverHandler", new SimpleChannelInboundHandler<ByteBuf>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx,
                                            ByteBuf msg) throws Exception {
                                        DubboFuture df = new DubboFuture();
                                        df.setData(msg);
                                        providerClient.writeAndFlush(df);
                                    }
                                });
                    }
                });
        if(Epoll.isAvailable()){
            serverBootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED)
                    .option(EpollChannelOption.TCP_QUICKACK, java.lang.Boolean.TRUE);
        }
        ChannelFuture future = serverBootstrap.bind(new InetSocketAddress(SERVER_PORT));
        future.syncUninterruptibly();
        logger.info("agent-provider-server:{}", future.channel().toString());
        registry = new EtcdRegistry(System.getProperty("etcd.url"));

        // provider connector
        InetSocketAddress address;
        if (RUN_MODE_LOCAL) {
            address = new InetSocketAddress(DUBBO_PROTOCOL_PORT);
        } else {
            address = new InetSocketAddress(SERVER_HOST, DUBBO_PROTOCOL_PORT);
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup.next())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.ALLOW_HALF_CLOSURE, Boolean.FALSE)
                .channel(AgentApp.socketChannel).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("dubbo-decoder", new DubboDecoder())
                                .addLast("dubbo-encoder", new DubboEncoder())
                                .addLast("agentClientReader",
                                        new SimpleChannelInboundHandler<DubboFuture>() {

                                            @Override
                                            protected void channelRead0(ChannelHandlerContext ctx,
                                                    DubboFuture df) {
                                                if (df.isEvent()) {
                                                    df.setEvent(true);
                                                    df.setTwoWay(false);
                                                    df.setRequest(false);
                                                    ctx.channel().writeAndFlush(df);
                                                    //FIXME ..心跳
                                                    return;
                                                }
                                                ByteBuf hashbs = df.getData();
                                                int id = (int) df.getId();
                                                Channel agentClient = agentClientMap.get(id);
                                                if (agentClient == null) {
                                                    logger.info("session closed id:{}",
                                                            String.valueOf(id));
                                                    return;
                                                }
                                                int hashLen = hashbs.readableBytes()
                                                        - (NEW_LINE_LEN * 2 + 1);
                                                int len = 4 + 4 + hashLen;
                                                ByteBuf buf = agentClient.alloc().directBuffer(len);
                                                buf.writeInt(len - 4);
                                                buf.writeInt(id);
                                                buf.writeBytes(hashbs, 1 + NEW_LINE_LEN, hashLen);
                                                agentClient.writeAndFlush(buf);
                                            }
                                        });
                    }
                });
        if(Epoll.isAvailable()){
            bootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED)
                    .option(EpollChannelOption.TCP_QUICKACK, java.lang.Boolean.TRUE);
        }
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000);
        for (;;) {
            future = bootstrap.connect(address);
            future.awaitUninterruptibly();
            if (future.isSuccess()) {
                providerClient = future.channel();
                break;
            }
            Thread.sleep(1000);
        }
        logger.info("dubbo-client:{}", future.channel().toString());

    }

    public static void setSystemPropertiesIfNull(String key, String value) {
        String pro = System.getProperty(key);
        if (pro == null || pro.length() == 0) {
            System.setProperty(key, value);
        }
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

    static class ConsumerAgentHandler extends ChannelDuplexHandler {
        private static final Logger     logger      = LoggerFactory
                .getLogger(ConsumerAgentHandler.class);

        private Map<String, String> parseParamString(String paramString) {
            Map<String, String> params = new HashMap<>();
            boolean findKey = true;
            int lastIndex = 0;
            String key = null;
            String value = null;
            for (int i = 0; i < paramString.length(); i++) {
                if (findKey) {
                    if (paramString.charAt(i) == '=') {
                        key = paramString.substring(lastIndex, i);
                        findKey = false;
                        lastIndex = i+1;
                    }
                }else{
                    if (paramString.charAt(i) == '&') {
                        value = paramString.substring(lastIndex, i);
                        findKey = true;
                        lastIndex = i+1;
                        params.put(key, value);
                    }
                }
            }
            if (lastIndex < paramString.length()) {
                value = paramString.substring(lastIndex);
                params.put(key, value);
            }
            return params;
        }

        private void encodeString(String s, ByteBuffer buf) {
            for (int i = 0; i < s.length(); i++) {
                buf.put((byte) s.charAt(i));
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            FullHttpRequest httpRequest = (FullHttpRequest) msg;
            try {
                ByteBuf contentByteBuf = httpRequest.content();
                String httpContent = contentByteBuf.toString(Encoding.UTF8);
                contentByteBuf.release();
                Map<String, String> values = parseParamString(httpContent);
                String interfaceName = values.get("interface");
                String methodName = values.get("method");
                String parameterTypesString = values.get("parameterTypesString");
                String parameter = values.get("parameter");
                if (parameter == null) {
                    parameter = "";
                }
                parameterTypesString = URLDecoder.decode(parameterTypesString, "UTF-8");
                EndpointGroup endpointGroup = (EndpointGroup) NettyUtil.getELV(interfaceName);
                Endpoint endpoint = endpointGroup.findFreeEndPoint();
                ByteBuffer CAReqBuf = (ByteBuffer) NettyUtil.getELV("CAReqBuf");
                if (CAReqBuf == null) {
                    CAReqBuf = ByteBuffer.allocateDirect(1024 * 4);
                    NettyUtil.setELV("CAReqBuf", CAReqBuf);
                }
                Attribute<Integer> chIdAttr = ctx.channel().attr(CHANNEL_ID_KEY);
                int chId = chIdAttr.get();
                endpoint.registHttpClient(chId);
                CAReqBuf.clear();
                int last = 0;
                int snbsLen;
                int mnbsLen;
                int mptbsLen;
                int mabsLen;
                encodeString(interfaceName, CAReqBuf);
                snbsLen = CAReqBuf.position();
                last = CAReqBuf.position();
                encodeString(methodName, CAReqBuf);
                mnbsLen = CAReqBuf.position() - last;
                last = CAReqBuf.position();
                encodeString(parameterTypesString, CAReqBuf);
                mptbsLen = CAReqBuf.position() - last;
                last = CAReqBuf.position();
                CharsetEncoder encoder = CharsetUtil.encoder(Encoding.UTF8);
                encoder.encode(CharBuffer.wrap(parameter), CAReqBuf, true);
                mabsLen = CAReqBuf.position() - last;
                CAReqBuf.flip();
                int len = 4 + 4 + 1 + 1 + 1 + 2 + snbsLen + mnbsLen + mptbsLen + mabsLen;
                ByteBuf buf = endpoint.getAllocator().directBuffer(len);
                buf.writeInt(len - 4);
                buf.writeInt(chId);
                buf.writeByte((byte) snbsLen);
                buf.writeByte((byte) mnbsLen);
                buf.writeByte((byte) mptbsLen);
                buf.writeShort(mabsLen);
                buf.writeBytes(CAReqBuf);
                endpoint.flushChannelFuture(chId, buf);
            } catch (Exception e) {
                logger.error("ConsumerAgentHandler invoke error", e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("consumerAgentHandler exceptionCaught error", cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    protected static class AgentDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                throws Exception {
            for (;;) {
                if (!in.isReadable()) {
                    return;
                }
                if (in.readableBytes() < 4) {
                    return;
                }
                int len = in.readInt();
                if (len < 0) {
                    if (len == -3) {
                        if (in.readableBytes() < 4) {
                            in.readerIndex(in.readerIndex() - 4);
                            return;
                        }
                        int sessionId = in.readInt();
                        AgentApp.agentClientMap.put(sessionId, ctx.channel());
                        return;
                    }else if(len == -4){
                        if (in.readableBytes() < 4) {
                            in.readerIndex(in.readerIndex() - 4);
                            return;
                        }
                        int sessionId = in.readInt();
                        AgentApp.agentClientMap.remove(sessionId);
                        return;
                    }
                }
                if (in.readableBytes() < len) {
                    in.readerIndex(in.readerIndex() - 4);
                    return;
                }
                ByteBuf res = ctx.alloc().directBuffer(len);
                in.readBytes(res);
                out.add(res);
            }
        }
    }

    protected static class DubboDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                throws Exception {
            int headerLen = DubboEncoder.HEADER_LENGTH;
            for (;;) {
                if (!in.isReadable()) {
                    return;
                }
                if (in.readableBytes() < headerLen) {
                    return;
                }
                int len = in.getInt(in.readerIndex() + 12);
                if (in.readableBytes() < len + headerLen) {
                    return;
                }
                DubboFuture f = new DubboFuture();
                in.skipBytes(2);
                byte byte2 = in.readByte();
                f.setRequest((byte2 & 0b10000000) != 0);
                f.setTwoWay((byte2 & 0b01000000) != 0);
                f.setEvent((byte2 & 0b00100000) != 0);
                f.setSerializationId((byte2 & 0b00011111));
                f.setStatus(in.readByte());
                f.setId(in.readLong());
                in.skipBytes(4);
                ByteBuf data = ctx.alloc().directBuffer(len);
                data.writeBytes(in, len);
                f.setData(data);
                out.add(f);
            }
        }
    }

}
