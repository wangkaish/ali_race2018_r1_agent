package com.alibaba.dubbo.performance.demo.agent.netty;

import java.io.Closeable;
import java.net.InetSocketAddress;

import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongHashSet;
import com.generallycloud.baseio.log.Logger;
import com.generallycloud.baseio.log.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.socket.SocketChannel;

public class Endpoint implements Closeable, Comparable<Endpoint> {

    private static final Logger logger            = LoggerFactory.getLogger(Endpoint.class);

    private final String        serviceName;
    private final String        host;
    private final int           port;
    private final int           scaleType;
    private final int           maxTask;
    private final int           recalSize;
    private EndpointGroup       group;
    private IntLongHashMap      rts               = new IntLongHashMap(512);
    public int                  rt                = 0;
    private int                 sum;
    private int                 time;
    private int                 remain;
    private Channel             channel;
    private ByteBufAllocator    allocator;
    private LongHashSet         registedSessionId = new LongHashSet();

    static final byte[]         httpHeader        = ("HTTP/1.1 200 OK\r\n"
            + "Content-Type: text/plain\r\n" + "Connection: keep-alive\r\n" + "Content-Length: ")
                    .getBytes();

    static final byte[]         errorHttpHeader   = ("HTTP/1.1 200 OK\r\n"
            + "Content-Type: text/plain\r\n" + "Connection: keep-alive\r\n"
            + "Content-Length: 0\r\n\r\n").getBytes();

    static final byte[]         RNRN              = "\r\n\r\n".getBytes();

    public void close() {
        channel.close();
    }

    public Endpoint(String serviceName, String host, int port, int scaleType, int maxTask) {
        this.host = host;
        this.port = port;
        this.scaleType = scaleType;
        this.maxTask = maxTask;
        this.recalSize = calcRecal(maxTask);
        this.serviceName = serviceName;
    }

    private int calcRecal(int maxTask) {
        if (isLarge()) {
            return maxTask * 3;
        } else if (isMedium()) {
            return maxTask * 2;
        } else {
            return maxTask * 1;
        }
    }

    public boolean isLarge() {
        return scaleType == 1;
    }

    public boolean isMedium() {
        return scaleType == 2;
    }

    public boolean isSmall() {
        return scaleType == 3;
    }

    public String getHost() {
        return host;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isFree() {
        return remain < maxTask;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return JSONObject.toJSONString(this);
    }

    public void connect(final EventLoop eventLoop) {

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoop).option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.ALLOW_HALF_CLOSURE, Boolean.FALSE)
                .channel(AgentApp.socketChannel).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("decoder", new AgentApp.AgentDecoder()).addLast(
                                "agentClientReader", new SimpleChannelInboundHandler<ByteBuf>() {

                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx,
                                            ByteBuf res) {
                                        remain--;
                                        int id = (int) res.readInt();
                                        long past = System.currentTimeMillis() - rts.get(id);
                                        if (past >= 50) {
                                            sum += past;
                                            time++;
                                            rt = sum / time;
                                            //                    if (time == 1024 * 8) {
                                            //                        sum = rt * 1024 * 4;
                                            //                        time = 1024 * 4;
                                            ////                        logger.info("type:{},rt:{}", scaleType, rt);
                                            //                    }
                                        }
                                        Channel session = NettyUtil.getSession(id);
                                        if (session == null || !session.isOpen()) {
                                            rts.remove(id);
                                            logger.info("session closed id:{}", id);
                                            return;
                                        }
                                        ByteBuf buf = session.alloc().directBuffer(256);
                                        int resRemain = res.readableBytes();
                                        if (resRemain > 50) {
                                            buf.writeBytes(errorHttpHeader);
                                        } else {
                                            buf.writeBytes(httpHeader);
                                            switch (resRemain) {
                                                case 1:
                                                    buf.writeByte((byte) '1');
                                                    break;
                                                case 2:
                                                    buf.writeByte((byte) '2');
                                                    break;
                                                case 3:
                                                    buf.writeByte((byte) '3');
                                                    break;
                                                case 4:
                                                    buf.writeByte((byte) '4');
                                                    break;
                                                case 5:
                                                    buf.writeByte((byte) '5');
                                                    break;
                                                case 6:
                                                    buf.writeByte((byte) '6');
                                                    break;
                                                case 7:
                                                    buf.writeByte((byte) '7');
                                                    break;
                                                case 8:
                                                    buf.writeByte((byte) '8');
                                                    break;
                                                case 9:
                                                    buf.writeByte((byte) '9');
                                                    break;
                                                case 10:
                                                    buf.writeByte((byte) '1');
                                                    buf.writeByte((byte) '0');
                                                    break;
                                                case 11:
                                                    buf.writeByte((byte) '1');
                                                    buf.writeByte((byte) '1');
                                                    break;
                                                default:
                                                    logger.info(String.valueOf(resRemain));
                                            }
                                            buf.writeBytes(RNRN);
                                            buf.writeBytes(res);
                                        }
                                        session.writeAndFlush(buf);
                                    }

                                });
                    }
                });
        if(Epoll.isAvailable()){
            bootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED)
                    .option(EpollChannelOption.TCP_QUICKACK, java.lang.Boolean.TRUE);
        }
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000);
        InetSocketAddress address = new InetSocketAddress(host, port);
        for (;;) {
            ChannelFuture future = bootstrap.connect(address);
            future.awaitUninterruptibly();
            if (future.isSuccess()) {
                channel = future.channel();
                allocator = channel.alloc();
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("agent-provider-client:{}", channel.toString());
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isConnected() {
        return channel != null && channel.isOpen();
    }

    public void flushChannelFuture(int id, ByteBuf future) {
        remain++;
        rts.put(id, System.currentTimeMillis());
        channel.writeAndFlush(future);
    }

    public void registHttpClient(int sessionId) {
        if (registedSessionId.contains(sessionId)) {
            return;
        }
        registedSessionId.add(sessionId);
        ByteBuf buf = channel.alloc().directBuffer(8);
        buf.writeInt(-3 << 32);
        buf.writeInt(sessionId);
        channel.writeAndFlush(buf);
    }

    public void deRegistHttpClient(int sessionId) {
        if (!registedSessionId.contains(sessionId)) {
            return;
        }
        registedSessionId.remove(sessionId);
        ByteBuf buf = channel.alloc().directBuffer(8);
        buf.writeInt(-4 << 32);
        buf.writeInt(sessionId);
        channel.writeAndFlush(buf);
    }

    public ByteBufAllocator getAllocator() {
        return allocator;
    }

    public void setAllocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    public boolean less(Endpoint o) {
        return rt < o.rt;
    }

    public boolean equals(Object o) {
        if (!(o instanceof Endpoint)) {
            return false;
        }
        Endpoint other = (Endpoint) o;
        return other.serviceName.equals(this.serviceName) && other.host.equals(this.host)
                && other.port == this.port;
    }

    public int hashCode() {
        return serviceName.hashCode() + host.hashCode() + port;
    }

    @Override
    protected Endpoint clone() {
        return new Endpoint(serviceName, host, port, scaleType, maxTask);
    }

    @Override
    public int compareTo(Endpoint o) {
        return o.scaleType - scaleType;//small在前
        //        return scaleType - o.scaleType;//large在前
    }

    public EndpointGroup getGroup() {
        return group;
    }

    public void setGroup(EndpointGroup group) {
        this.group = group;
    }

    public int getScaleType() {
        return scaleType;
    }

    public int getRt() {
        return rt;
    }

    public int getMaxTask() {
        return maxTask;
    }

    public int getRecalSize() {
        return recalSize;
    }

}
