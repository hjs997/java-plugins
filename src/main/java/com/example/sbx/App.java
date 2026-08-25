package com.example.sbx;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class App {

    // ===== 配置区 =====
    private static final String UUID = "08431c4e-d071-40c2-872b-85f7049f5d80";
    private static final int LISTEN_PORT = 24614;   // 面板分配的端口
    private static final String WS_PATH = "/wszxl";    // WebSocket 路径
    private static final String SUB_PATH = "/sub";  // 获取节点链接的私密路径
    // ==================

    private static final byte[] UUID_BYTES = hexStringToByteArray(UUID.replace("-", ""));
    private static final String PROTOCOL_UUID = UUID.replace("-", "");
    private static volatile String cachedIp = null;

    private static final List<String> BLOCKED_DOMAINS = Arrays.asList(
            "speedtest.net", "fast.com", "speedtest.cn", "speed.cloudflare.com",
            "speedof.me", "testmy.net", "bandwidth.place", "speed.io",
            "librespeed.org", "speedcheck.org");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;
    private static Channel serverChannel;

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(4))
            .build();

    public static void main(String[] args) {
        start();
        try {
            if (serverChannel != null) {
                serverChannel.closeFuture().sync();
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public static void start() {
        if (!RUNNING.compareAndSet(false, true)) return;

        try {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            // 45秒无读写自动断开，防止MC面板内存泄露
                            p.addLast(new IdleStateHandler(45, 45, 0));
                            p.addLast(new HttpServerCodec());
                            p.addLast(new HttpObjectAggregator(65536));
                            p.addLast(new SubHttpHandler());
                            p.addLast(new WebSocketServerProtocolHandler(WS_PATH, null, false));
                            p.addLast(new WebSocketFrameAggregator(16 * 1024 * 1024));
                            p.addLast(new WebSocketProxyHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_RCVBUF, 512 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 512 * 1024)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 1024 * 1024, 4 * 1024 * 1024));

            serverChannel = b.bind(LISTEN_PORT).sync().channel();
        } catch (Exception ignored) {
            stop();
        }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try {
            if (serverChannel != null) {
                serverChannel.close();
                serverChannel = null;
            }
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
        } catch (Exception ignored) {
        }
    }

    // --- 异步获取真实 IP 并输出订阅 ---
    static class SubHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            String uri = req.uri();

            if (SUB_PATH.equals(uri)) {
                if (cachedIp != null) {
                    sendResponse(ctx, cachedIp);
                    return;
                }

                // 异步拉取 IP，不阻塞 Netty Worker 线程
                HttpRequest ipReq = HttpRequest.newBuilder()
                        .uri(URI.create("https://api-ipv4.ip.sb/ip"))
                        .timeout(Duration.ofSeconds(4))
                        .build();

                HTTP_CLIENT.sendAsync(ipReq, HttpResponse.BodyHandlers.ofString())
                        .thenAccept(resp -> {
                            if (resp.statusCode() == 200) {
                                cachedIp = resp.body().trim();
                                sendResponse(ctx, cachedIp);
                            } else {
                                sendResponse(ctx, "Fetch_IP_Failed");
                            }
                        })
                        .exceptionally(ex -> {
                            sendResponse(ctx, "Fetch_IP_Error");
                            return null;
                        });
            } else if (!uri.startsWith(WS_PATH)) {
                // 伪装 404，防止被轻易探测
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                        Unpooled.copiedBuffer("404 Not Found", StandardCharsets.UTF_8));
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            } else {
                req.retain();
                ctx.fireChannelRead(req);
            }
        }

        private void sendResponse(ChannelHandlerContext ctx, String ip) {
            String vlessUrl = String.format(
                    "vless://%s@%s:%d?encryption=none&security=none&type=ws&host=&path=%s#MC_Node\n",
                    UUID, ip, LISTEN_PORT, WS_PATH.replace("/", "%2F")
            );
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(vlessUrl, StandardCharsets.UTF_8));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                ctx.close();
            }
        }
    }

    // --- 代理核心逻辑 ---
    static class WebSocketProxyHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
        private static final long MAX_PENDING_BYTES = 2L * 1024 * 1024;

        private Channel outboundChannel;
        private boolean connected = false;
        private boolean connecting = false;
        private boolean protocolIdentified = false;
        private final Queue<ByteBuf> pendingOutboundWrites = new ArrayDeque<>();
        private long pendingOutboundBytes = 0;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf content = frame.content();

                if (!protocolIdentified) {
                    byte[] data = new byte[content.readableBytes()];
                    content.getBytes(content.readerIndex(), data);
                    handleFirstMessage(ctx, data);
                } else if (outboundChannel != null && outboundChannel.isActive()) {
                    relayToTarget(ctx, content.retain());
                } else if (connecting) {
                    queuePendingOutbound(ctx, content.retain());
                } else {
                    closeBoth(ctx);
                }
            } else if (frame instanceof CloseWebSocketFrame) {
                closeBoth(ctx);
            }
        }

        private void relayToTarget(ChannelHandlerContext ctx, ByteBuf data) {
            if (outboundChannel == null || !outboundChannel.isActive()) {
                data.release();
                closeBoth(ctx);
                return;
            }
            outboundChannel.write(data).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    closeBoth(ctx);
                }
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.flush();
            }
            ctx.fireChannelReadComplete();
        }

        private void queuePendingOutbound(ChannelHandlerContext ctx, ByteBuf data) {
            int readableBytes = data.readableBytes();
            if (pendingOutboundBytes + readableBytes > MAX_PENDING_BYTES) {
                data.release();
                closeBoth(ctx);
                return;
            }
            pendingOutboundWrites.add(data);
            pendingOutboundBytes += readableBytes;
        }

        private void flushPendingOutbound(ChannelHandlerContext ctx) {
            while (!pendingOutboundWrites.isEmpty()) {
                if (outboundChannel == null || !outboundChannel.isActive()) {
                    releasePendingOutbound();
                    closeBoth(ctx);
                    return;
                }
                ByteBuf data = pendingOutboundWrites.poll();
                pendingOutboundBytes -= data.readableBytes();
                outboundChannel.write(data).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        closeBoth(ctx);
                    }
                });
            }
            if (outboundChannel != null) {
                outboundChannel.flush();
            }
        }

        private void releasePendingOutbound() {
            ByteBuf data;
            while ((data = pendingOutboundWrites.poll()) != null) {
                data.release();
            }
            pendingOutboundBytes = 0;
        }

        private void closeBoth(ChannelHandlerContext ctx) {
            releasePendingOutbound();
            if (outboundChannel != null && outboundChannel.isOpen()) {
                outboundChannel.close();
            }
            if (ctx.channel().isOpen()) {
                ctx.close();
            }
        }

        private void handleFirstMessage(ChannelHandlerContext ctx, byte[] data) {
            if (data.length > 18 && data[0] == 0x00) {
                boolean uuidMatch = true;
                for (int i = 0; i < 16; i++) {
                    if (data[i + 1] != UUID_BYTES[i]) {
                        uuidMatch = false;
                        break;
                    }
                }
                if (uuidMatch && handleVless(ctx, data)) {
                    protocolIdentified = true;
                    return;
                }
            }

            if (data.length >= 56) {
                byte[] hashBytes = Arrays.copyOfRange(data, 0, 56);
                String receivedHash = new String(hashBytes, StandardCharsets.US_ASCII);
                String expectedHash = sha224Hex(UUID);
                String expectedHash2 = sha224Hex(PROTOCOL_UUID);

                if ((receivedHash.equals(expectedHash) || receivedHash.equals(expectedHash2)) && handleTrojan(ctx, data)) {
                    protocolIdentified = true;
                    return;
                }
            }

            if (data.length > 2 && (data[0] == 0x01 || data[0] == 0x03 || data[0] == 0x04)) {
                if (handleShadowsocks(ctx, data)) {
                    protocolIdentified = true;
                    return;
                }
            }

            ctx.close();
        }

        private boolean handleVless(ChannelHandlerContext ctx, byte[] data) {
            try {
                int addonsLength = data[17] & 0xFF;
                int offset = 18 + addonsLength;
                if (offset + 1 > data.length) return false;

                byte command = data[offset];
                if (command != 0x01) return false; // 0x01 = TCP
                offset++;
                if (offset + 2 > data.length) return false;

                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;
                if (offset >= data.length) return false;

                byte atyp = data[offset];
                offset++;
                String host;
                int addressLength;

                if (atyp == 0x01) {
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d",
                            data[offset] & 0xFF, data[offset + 1] & 0xFF,
                            data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x02) {
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x03) {
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }

                offset += addressLength;

                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }

                // VLESS 响应头：0x00 代表版本，0x00 代表附加信息长度为 0
                ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{0x00, 0x00})));

                final byte[] remainingData = (offset < data.length) ? Arrays.copyOfRange(data, offset, data.length) : new byte[0];
                connectToTarget(ctx, host, port, remainingData);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private boolean handleTrojan(ChannelHandlerContext ctx, byte[] data) {
            try {
                int offset = 56;
                while (offset < data.length && (data[offset] == '\r' || data[offset] == '\n')) offset++;
                if (offset >= data.length || data[offset] != 0x01) return false;
                offset++;
                if (offset >= data.length) return false;

                byte atyp = data[offset];
                offset++;
                String host;
                int addressLength;

                if (atyp == 0x01) {
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d",
                            data[offset] & 0xFF, data[offset + 1] & 0xFF,
                            data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x03) {
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x04) {
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }

                offset += addressLength;
                if (offset + 2 > data.length) return false;

                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;

                while (offset < data.length && (data[offset] == '\r' || data[offset] == '\n')) offset++;

                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }

                final byte[] remainingData = (offset < data.length) ? Arrays.copyOfRange(data, offset, data.length) : new byte[0];
                connectToTarget(ctx, host, port, remainingData);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private boolean handleShadowsocks(ChannelHandlerContext ctx, byte[] data) {
            try {
                int offset = 0;
                byte atyp = data[offset];
                offset++;
                String host;
                int addressLength;

                if (atyp == 0x01) {
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d",
                            data[offset] & 0xFF, data[offset + 1] & 0xFF,
                            data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x03) {
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x04) {
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }

                offset += addressLength;
                if (offset + 2 > data.length) return false;

                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;

                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }

                final byte[] remainingData = (offset < data.length) ? Arrays.copyOfRange(data, offset, data.length) : new byte[0];
                connectToTarget(ctx, host, port, remainingData);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private void connectToTarget(ChannelHandlerContext ctx, String host, int port, byte[] remainingData) {
            if (connecting || connected) {
                closeBoth(ctx);
                return;
            }

            final byte[] dataToSend = remainingData;
            connecting = true;
            ctx.channel().config().setAutoRead(false);

            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 8000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_RCVBUF, 512 * 1024)
                    .option(ChannelOption.SO_SNDBUF, 512 * 1024)
                    .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 1024 * 1024, 4 * 1024 * 1024))
                    .option(ChannelOption.AUTO_READ, false)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new TargetHandler(ctx.channel(), dataToSend));
                        }
                    });

            ChannelFuture f = b.connect(host, port);
            outboundChannel = f.channel();

            f.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    connected = true;
                    connecting = false;
                    flushPendingOutbound(ctx);
                    future.channel().config().setAutoRead(true);
                    if (ctx.channel().isActive()) {
                        ctx.channel().config().setAutoRead(true);
                    }
                } else {
                    connecting = false;
                    closeBoth(ctx);
                }
            });
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.config().setAutoRead(ctx.channel().isWritable());
            }
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                closeBoth(ctx);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            closeBoth(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            closeBoth(ctx);
        }
    }

    static class TargetHandler extends ChannelInboundHandlerAdapter {
        private final Channel inboundChannel;
        private final byte[] remainingData;

        public TargetHandler(Channel inboundChannel, byte[] remainingData) {
            this.inboundChannel = inboundChannel;
            this.remainingData = remainingData;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (remainingData != null && remainingData.length > 0) {
                ctx.writeAndFlush(Unpooled.wrappedBuffer(remainingData)).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        ctx.close();
                    }
                });
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof ByteBuf) {
                    ByteBuf buf = (ByteBuf) msg;
                    if (inboundChannel.isActive()) {
                        inboundChannel.write(new BinaryWebSocketFrame(buf.retain()))
                                .addListener((ChannelFutureListener) future -> {
                                    if (!future.isSuccess()) {
                                        ctx.close();
                                    }
                                });
                    } else {
                        ctx.close();
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) {
                inboundChannel.flush();
            }
            ctx.fireChannelReadComplete();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) {
                inboundChannel.config().setAutoRead(ctx.channel().isWritable());
            }
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) {
                inboundChannel.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static String sha224Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-224");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException ignored) {
            return "";
        }
    }

    private static boolean isBlockedDomain(String host) {
        if (host == null || host.isEmpty()) return false;
        String hostLower = host.toLowerCase();
        return BLOCKED_DOMAINS.stream().anyMatch(blocked ->
                hostLower.equals(blocked) || hostLower.endsWith("." + blocked));
    }
}
