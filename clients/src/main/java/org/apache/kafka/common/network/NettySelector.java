/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.AttributeKey;

public class NettySelector implements Selectable, AutoCloseable {
    private static final AttributeKey<String> CONNECTION_ID_ATTR = AttributeKey.valueOf("kafkaConnectionId");

    private final Logger log;
    private final int maxReceiveSize;
    private final NioEventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private final Semaphore pollSignals = new Semaphore(0);
    private final ConcurrentHashMap<String, ConnectionContext> connections = new ConcurrentHashMap<>();
    private final Set<String> explicitlyMuted = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<String> pendingConnected = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<NetworkReceive> pendingCompletedReceives = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<NetworkSend> pendingCompletedSends = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<DisconnectedEvent> pendingDisconnected = new ConcurrentLinkedQueue<>();
    private volatile boolean closed = false;
    private volatile boolean muteAll = false;

    private List<NetworkSend> completedSends = new ArrayList<>();
    private List<NetworkReceive> completedReceives = new ArrayList<>();
    private List<String> connected = new ArrayList<>();
    private Map<String, ChannelState> disconnected = new ConcurrentHashMap<>();

    public NettySelector(int maxReceiveSize,
                         long connectionMaxIdleMs,
                         int failedAuthenticationDelayMs,
                         Metrics metrics,
                         Time time,
                         String metricGrpPrefix,
                         Map<String, String> metricTags,
                         boolean metricsPerConnection,
                         boolean recordTimePerConnection,
                         ChannelBuilder channelBuilder,
                         MemoryPool memoryPool,
                         LogContext logContext) {
        this.maxReceiveSize = maxReceiveSize;
        this.log = logContext.logger(NettySelector.class);
        this.eventLoopGroup = new NioEventLoopGroup(1);
        this.bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) {
                    channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(maxFrameLength(), 0, 4, 0, 4));
                    channel.pipeline().addLast(new NettyInboundHandler());
                }
            });
    }

    public NettySelector(int maxReceiveSize,
                         long connectionMaxIdleMs,
                         Metrics metrics,
                         Time time,
                         String metricGrpPrefix,
                         Map<String, String> metricTags,
                         boolean metricsPerConnection,
                         boolean recordTimePerConnection,
                         ChannelBuilder channelBuilder,
                         MemoryPool memoryPool,
                         LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, Selector.NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags,
            metricsPerConnection, recordTimePerConnection, channelBuilder, memoryPool, logContext);
    }

    public NettySelector(int maxReceiveSize,
                         long connectionMaxIdleMs,
                         int failedAuthenticationDelayMs,
                         Metrics metrics,
                         Time time,
                         String metricGrpPrefix,
                         Map<String, String> metricTags,
                         boolean metricsPerConnection,
                         ChannelBuilder channelBuilder,
                         LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, false, channelBuilder, MemoryPool.NONE, logContext);
    }

    public NettySelector(int maxReceiveSize,
                         long connectionMaxIdleMs,
                         Metrics metrics,
                         Time time,
                         String metricGrpPrefix,
                         Map<String, String> metricTags,
                         boolean metricsPerConnection,
                         ChannelBuilder channelBuilder,
                         LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, Selector.NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, channelBuilder, logContext);
    }

    public NettySelector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    public NettySelector(long connectionMaxIdleMS, int failedAuthenticationDelayMs, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        ensureOpen();
        ConnectionContext existing = connections.putIfAbsent(id, new ConnectionContext(id));
        if (existing != null) {
            throw new IllegalStateException("There is already a connection for id " + id);
        }

        ChannelFuture connectFuture;
        try {
            connectFuture = bootstrap.connect(address);
        } catch (RuntimeException e) {
            connections.remove(id);
            throw new IOException("Failed to initiate connection to " + address, e);
        }

        ConnectionContext context = Objects.requireNonNull(connections.get(id));
        context.connectFuture = connectFuture;
        connectFuture.channel().attr(CONNECTION_ID_ATTR).set(id);
        connectFuture.addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = connectFuture.channel();
                context.channel = channel;
                applyMuteState(id, channel);
            } else {
                connections.remove(id);
                context.enqueueDisconnect(ChannelState.NOT_CONNECTED);
            }
        });
    }

    @Override
    public void wakeup() {
        signalPoll();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        connections.values().forEach(context -> context.close(ChannelState.LOCAL_CLOSE));
        connections.clear();
        eventLoopGroup.shutdownGracefully();
        signalPoll();
    }

    @Override
    public void close(String id) {
        ConnectionContext context = connections.remove(id);
        if (context != null) {
            context.close(ChannelState.LOCAL_CLOSE);
        }
    }

    @Override
    public void send(NetworkSend send) {
        ensureOpen();
        String id = send.destinationId();
        ConnectionContext context = connections.get(id);
        if (context == null || !context.isReady()) {
            throw new IllegalStateException("Attempt to send to disconnected channel " + id);
        }

        ByteBuf payload;
        try {
            payload = serializeSend(send.send());
        } catch (IOException e) {
            throw new KafkaException("Failed to serialize send for " + id, e);
        }

        context.channel.writeAndFlush(payload).addListener(future -> {
            if (future.isSuccess()) {
                pendingCompletedSends.add(send);
                signalPoll();
            } else {
                log.debug("Send failed for {}", id, future.cause());
                context.close(ChannelState.FAILED_SEND);
            }
        });
    }

    @Override
    public void poll(long timeout) throws IOException {
        ensureOpen();
        if (isPendingEventsEmpty() && timeout > 0) {
            try {
                if (isPendingEventsEmpty()) {
                    if (pollSignals.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
                        pollSignals.drainPermits();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while polling", e);
            }
        }

        List<NetworkSend> currentCompletedSends = new ArrayList<>();
        for (NetworkSend send = pendingCompletedSends.poll(); send != null; send = pendingCompletedSends.poll()) {
            currentCompletedSends.add(send);
        }
        completedSends = currentCompletedSends;

        List<NetworkReceive> currentCompletedReceives = new ArrayList<>();
        for (NetworkReceive receive = pendingCompletedReceives.poll(); receive != null; receive = pendingCompletedReceives.poll()) {
            currentCompletedReceives.add(receive);
        }
        completedReceives = currentCompletedReceives;

        List<String> currentConnected = new ArrayList<>();
        for (String id = pendingConnected.poll(); id != null; id = pendingConnected.poll()) {
            currentConnected.add(id);
        }
        connected = currentConnected;

        Map<String, ChannelState> currentDisconnected = new ConcurrentHashMap<>();
        for (DisconnectedEvent event = pendingDisconnected.poll(); event != null; event = pendingDisconnected.poll()) {
            currentDisconnected.put(event.id, event.channelState);
        }
        disconnected = currentDisconnected;
    }

    @Override
    public List<NetworkSend> completedSends() {
        return completedSends;
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return completedReceives;
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return disconnected;
    }

    @Override
    public List<String> connected() {
        return connected;
    }

    @Override
    public void mute(String id) {
        explicitlyMuted.add(id);
        ConnectionContext context = connections.get(id);
        if (context != null && context.channel != null) {
            context.channel.config().setAutoRead(false);
        }
    }

    @Override
    public void unmute(String id) {
        explicitlyMuted.remove(id);
        ConnectionContext context = connections.get(id);
        if (context != null && context.channel != null && !muteAll) {
            context.channel.config().setAutoRead(true);
        }
    }

    @Override
    public void muteAll() {
        muteAll = true;
        for (ConnectionContext context : connections.values()) {
            if (context.channel != null) {
                context.channel.config().setAutoRead(false);
            }
        }
    }

    @Override
    public void unmuteAll() {
        muteAll = false;
        for (ConnectionContext context : connections.values()) {
            Channel channel = context.channel;
            if (channel != null && !explicitlyMuted.contains(context.id)) {
                channel.config().setAutoRead(true);
            }
        }
    }

    @Override
    public boolean isChannelReady(String id) {
        ConnectionContext context = connections.get(id);
        return context != null && context.isReady();
    }

    private ByteBuf serializeSend(Send send) throws IOException {
        if (send.size() > Integer.MAX_VALUE) {
            throw new IOException("Send is too large: " + send.size());
        }

        InMemoryTransferableChannel channel = new InMemoryTransferableChannel((int) send.size());
        while (!send.completed()) {
            send.writeTo(channel);
        }
        ByteBuffer payload = channel.payload();
        return Unpooled.wrappedBuffer(payload);
    }

    private void onConnected(String id, Channel channel) {
        ConnectionContext context = connections.get(id);
        if (context == null) {
            return;
        }
        context.connected = true;
        context.channel = channel;
        pendingConnected.add(id);
        signalPoll();
    }

    private void onDisconnected(String id, ChannelState channelState) {
        ConnectionContext context = connections.remove(id);
        if (context != null) {
            context.enqueueDisconnect(channelState);
        }
    }

    private void onReceive(String id, ByteBuf message) {
        byte[] payload = new byte[message.readableBytes()];
        message.readBytes(payload);
        pendingCompletedReceives.add(new NetworkReceive(id, ByteBuffer.wrap(payload)));
        signalPoll();
    }

    private void signalPoll() {
        if (pollSignals.availablePermits() == 0) {
            pollSignals.release();
        }
    }

    private boolean isPendingEventsEmpty() {
        return pendingConnected.isEmpty() &&
            pendingCompletedReceives.isEmpty() &&
            pendingCompletedSends.isEmpty() &&
            pendingDisconnected.isEmpty();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Netty selector is closed");
        }
    }

    private void applyMuteState(String id, Channel channel) {
        channel.config().setAutoRead(!muteAll && !explicitlyMuted.contains(id));
    }

    private int maxFrameLength() {
        if (maxReceiveSize == NetworkReceive.UNLIMITED) {
            return Integer.MAX_VALUE;
        }
        return maxReceiveSize;
    }

    private final class NettyInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            String id = ctx.channel().attr(CONNECTION_ID_ATTR).get();
            if (id != null) {
                onConnected(id, ctx.channel());
            }
            ctx.fireChannelActive();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            String id = ctx.channel().attr(CONNECTION_ID_ATTR).get();
            if (id != null) {
                onReceive(id, msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            String id = ctx.channel().attr(CONNECTION_ID_ATTR).get();
            if (id != null) {
                ConnectionContext context = connections.get(id);
                ChannelState state = ChannelState.READY;
                if (context != null && context.requestedDisconnectState != null) {
                    state = context.requestedDisconnectState;
                }
                onDisconnected(id, state);
            }
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("Netty channel failure", cause);
            ctx.close();
        }
    }

    private final class ConnectionContext {
        private final String id;
        private final AtomicBoolean disconnectEnqueued = new AtomicBoolean(false);
        private volatile ChannelFuture connectFuture;
        private volatile Channel channel;
        private volatile boolean connected = false;
        private volatile ChannelState requestedDisconnectState;

        private ConnectionContext(String id) {
            this.id = id;
        }

        private boolean isReady() {
            return connected && channel != null && channel.isActive();
        }

        private void close(ChannelState channelState) {
            requestedDisconnectState = channelState;
            Channel existingChannel = channel;
            if (existingChannel != null) {
                existingChannel.close();
            } else {
                enqueueDisconnect(channelState);
            }
            ChannelFuture pendingConnect = connectFuture;
            if (pendingConnect != null && !pendingConnect.isDone()) {
                pendingConnect.cancel(true);
            }
        }

        private void enqueueDisconnect(ChannelState channelState) {
            if (disconnectEnqueued.compareAndSet(false, true)) {
                pendingDisconnected.add(new DisconnectedEvent(id, channelState));
                signalPoll();
            }
        }
    }

    private static final class DisconnectedEvent {
        private final String id;
        private final ChannelState channelState;

        private DisconnectedEvent(String id, ChannelState channelState) {
            this.id = id;
            this.channelState = channelState;
        }
    }

    private static final class InMemoryTransferableChannel implements TransferableChannel {
        private final ByteBuffer target;
        private boolean open = true;

        private InMemoryTransferableChannel(int size) {
            this.target = ByteBuffer.allocate(size);
        }

        @Override
        public int write(ByteBuffer src) throws ClosedChannelException {
            ensureOpen();
            int bytes = src.remaining();
            target.put(src);
            return bytes;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws ClosedChannelException {
            ensureOpen();
            long total = 0;
            for (int i = offset; i < offset + length; i++) {
                total += write(srcs[i]);
            }
            return total;
        }

        @Override
        public long write(ByteBuffer[] srcs) throws ClosedChannelException {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean hasPendingWrites() {
            return false;
        }

        @Override
        public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
            ensureOpen();
            long transferred = 0;
            ByteBuffer scratch = ByteBuffer.allocate((int) Math.min(8192, count));
            while (transferred < count) {
                scratch.clear();
                int limit = (int) Math.min(scratch.capacity(), count - transferred);
                scratch.limit(limit);
                int read = fileChannel.read(scratch, position + transferred);
                if (read <= 0) {
                    break;
                }
                transferred += read;
                scratch.flip();
                write(scratch);
            }
            return transferred;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        private ByteBuffer payload() {
            ByteBuffer payload = target.duplicate();
            payload.flip();
            return payload;
        }

        private void ensureOpen() throws ClosedChannelException {
            if (!open) {
                throw new ClosedChannelException();
            }
        }
    }
}
