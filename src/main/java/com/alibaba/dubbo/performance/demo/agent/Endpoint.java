package com.alibaba.dubbo.performance.demo.agent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.dubbo.performance.demo.agent.AgentApp.SetTCP_NODELAYSEListener;
import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntLongHashMap;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.component.ChannelActiveIdleEventListener;
import com.generallycloud.baseio.component.ChannelContext;
import com.generallycloud.baseio.component.ChannelEventListenerAdapter;
import com.generallycloud.baseio.component.IoEventHandle;
import com.generallycloud.baseio.component.LoggerChannelOpenListener;
import com.generallycloud.baseio.component.NioEventLoop;
import com.generallycloud.baseio.component.NioEventLoopTask;
import com.generallycloud.baseio.component.NioSocketChannel;
import com.generallycloud.baseio.component.ReConnector;
import com.generallycloud.baseio.protocol.Future;

public final class Endpoint implements Closeable, Comparable<Endpoint> {

//    private static final Logger logger            = LoggerFactory.getLogger(Endpoint.class);

    private final String        serviceName;
    private final String        host;
    private final int           port;
    private final int           scaleType;
    private final int           maxTask;
    private final int           recalSize;
    private EndpointGroup       group;
    private IntLongHashMap      rts               = new IntLongHashMap(512);
    private int                 rt                = 0;
    private int                 sum;
    private int                 time;
    private int                 remain;
    private int                 all = 0;
    private NioSocketChannel    channel;
    private ByteBufAllocator    allocator;
    private ReConnector         connector;
    private IntHashSet          registedChannelId = new IntHashSet();

    static final byte[]         httpHeader        = ("HTTP/1.1 200 OK\r\n"
            + "Content-Type: text/plain\r\n" + "Connection: keep-alive\r\n" + "Content-Length: ")
                    .getBytes();

    static final byte[]         errorHttpHeader   = ("HTTP/1.1 200 OK\r\n"
            + "Content-Type: text/plain\r\n" + "Connection: keep-alive\r\n"
            + "Content-Length: 0\r\n\r\n").getBytes();

    static final byte[]         RNRN              = "\r\n\r\n".getBytes();

    public void close() {
        CloseUtil.close(connector);
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

    public void connect(final NioEventLoop eventLoop) {
        ChannelContext context = new ChannelContext(host, port);
        connector = new ReConnector(context, eventLoop);
        connector.setRetryTime(1000);
        context.setIoEventHandle(new IoEventHandle() {

            @Override
            public final void accept(NioSocketChannel agentChannel, Future future)
                    throws Exception {
                remain--;
                all++;
                AgentFuture fx = (AgentFuture) future;
                ByteBuf res = fx.getData();
                int id = res.getInt();
                long past = System.currentTimeMillis() - rts.get(id);
                if (past >= 50) {
                    sum += past;
                    time++;
                    rt = sum / time;
                    //                    if (time == 1024 * 8) {
                    //                        sum = sum / 2;
                    //                        time = time / 2;
                    ////                        logger.info("type:{},rt:{}", scaleType, rt);
                    //                    }
                }
//                if (all % 2000 == 0) {
//                    logger.info("e:{},rt:{},past:{}", 
//                            String.valueOf(getScaleType()),
//                            String.valueOf(getRt()),
//                            String.valueOf(past));
//                }
                NioSocketChannel channel = eventLoop.getChannel(id);
                if (channel == null || channel.isClosed()) {
                    rts.remove(id);
//                    logger.info("channel closed id:{}", id);
                    return;
                }
                ByteBuf buf = channel.allocator().allocate(256);
                int resRemain = res.remaining();
                if (resRemain > 50) {
                    res.release(res.getReleaseVersion());
                    buf.put(errorHttpHeader);
                } else {
                    buf.put(httpHeader);
                    buf.put(Integer.toString(resRemain).getBytes());
                    buf.put(RNRN);
                    buf.read(res);
                    res.release(res.getReleaseVersion());
                }
                channel.flush(buf.flip());
            }
        });
        context.addChannelEventListener(new ChannelEventListenerAdapter() {

            @Override
            public void channelOpened(NioSocketChannel channel) throws Exception {
                Endpoint.this.channel = channel;
                Endpoint.this.allocator = channel.allocator();
            }
        });
        context.addChannelEventListener(new SetTCP_NODELAYSEListener());
        context.addChannelEventListener(new LoggerChannelOpenListener());
        if (!AgentApp.RUN_MODE_LOCAL) {
            context.addChannelIdleEventListener(new ChannelActiveIdleEventListener());
        }
        context.setProtocolCodec(new AgentCodec());
        connector.connect();
    }

    public NioSocketChannel getChannel() {
        return channel;
    }

    public boolean isConnected() {
        return channel != null && channel.isOpened();
    }
    
    private boolean needAddTask = true;
    
    private final List<ByteBuf> futures = new ArrayList<>(200);

    public void flushChannelFuture(int id, ByteBuf buf) {
        final NioSocketChannel channel = this.channel;
        remain++;
        rts.put(id, System.currentTimeMillis());
        if (AgentApp.BATCH_FLUSH) {
            futures.add(buf);
            if (needAddTask) {
                needAddTask = false;
                channel.getEventLoop().dispatchAfterLoop(new NioEventLoopTask() {
                    
                    @Override
                    public void fireEvent(NioEventLoop eventLoop) throws IOException {
                        channel.flush(futures);
                        futures.clear();
                        needAddTask = true;
                    }
                });
            }
        }else{
            channel.flush(buf);
        }
    }

    public void registHttpClient(int channelId) {
        if (registedChannelId.contains(channelId)) {
            return;
        }
        registedChannelId.add(channelId);
        ByteBuf buf = channel.allocator().allocate(8);
        buf.putInt(-3 << 32);
        buf.putInt(channelId);
        channel.flush(buf.flip());
    }

    public void deRegistHttpClient(int channelId) {
        if (!registedChannelId.contains(channelId)) {
            return;
        }
        registedChannelId.remove(channelId);
        ByteBuf buf = channel.allocator().allocate(8);
        buf.putInt(-4 << 32);
        buf.putInt(channelId);
        channel.flush(buf.flip());
    }

    public ByteBufAllocator allocator() {
        return allocator;
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
