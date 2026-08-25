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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class App {

    // ================= 核心配置区 =================
    private static final String UUID = "e8b68075-4a22-48d1-85b8-66fa1e063713";
    
    // 👇 1. 必填：你的 Cloudflare Tunnel 长串 Token
    private static final String CF_TOKEN = "eyJhIjoiNTQzZDRkZTQzYjBkMjFhY2I0OTgyMmJkZGI1NzdkOTQiLCJ0IjoiZWMwNDM4MjQtZWQ5OS00NTZlLWJiMmEtMDgwZTJiNmZjMTY4IiwicyI6Ik5EWTVZMlkxTVRJdFpqUmhaQzAwTnpRMkxUbGpPVEV0TlRsbE1UVmhNMlU1WmpJMCJ9"; 
    
    // 👇 2. 必填：面板分配给你的真实 MC 端口 (保活机器人需要去 Ping 它)
    private static final int MC_REAL_PORT = 24614; 

    // 本地内部监听端口 (仅供 CF 隧道转发使用，绝对不与 MC 端口冲突)
    private static final int LISTEN_PORT = 30000;   
    private static final String WS_PATH = "/ws";    
    // ==============================================

    private static final byte[] UUID_BYTES = hexStringToByteArray(UUID.replace("-", ""));
    private static final String PROTOCOL_UUID = UUID.replace("-", "");

    private static final List<String> BLOCKED_DOMAINS = Arrays.asList(
            "speedtest.net", "fast.com", "speedtest.cn", "speed.cloudflare.com",
            "speedof.me", "testmy.net", "bandwidth.place", "speed.io",
            "librespeed.org", "speedcheck.org");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;
    private static Channel serverChannel;
    private static Process tunnelProcess = null;

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

        // 1. 启动极限伪装版的 Cloudflare 隧道守护进程
        startCloudflareTunnelDaemon();

        // 2. 启动本地 MC TCP 强行心跳保活机器人 (防休眠)
        startMCKeepAliveBot(MC_REAL_PORT);

        // 3. 阅后即焚：打印本地模板节点配置
        printNodeTemplateAndBurn();

        // 4. 启动 Netty 代理核心，仅绑定 127.0.0.1 内部回环
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
                            p.addLast(new IdleStateHandler(45, 45, 0));
                            p.addLast(new HttpServerCodec());
                            p.addLast(new HttpObjectAggregator(65536));
                            p.addLast(new WebSocketServerProtocolHandler(WS_PATH, null, false));
                            p.addLast(new WebSocketFrameAggregator(16 * 1024 * 1024));
                            p.addLast(new WebSocketProxyHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            serverChannel = b.bind("127.0.0.1", LISTEN_PORT).sync().channel();
        } catch (Exception ignored) {
            stop();
        }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try {
            if (tunnelProcess != null) tunnelProcess.destroyForcibly();
            if (serverChannel != null) serverChannel.close();
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
        } catch (Exception ignored) {}
    }

    // ========================================================
    // 模块 1：极限伪装 CF 隧道 (内存欺骗 + Bash 进程重写)
    // ========================================================
    private static void startCloudflareTunnelDaemon() {
        if (CF_TOKEN == null || CF_TOKEN.length() < 50) return;

        Thread watchdogThread = new Thread(() -> {
            while (RUNNING.get()) {
                try {
                    if (tunnelProcess == null || !tunnelProcess.isAlive()) {
                        String arch = System.getProperty("os.arch").toLowerCase();
                        String dlUrl = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64";
                        if (arch.contains("arm") || arch.contains("aarch64")) {
                            dlUrl = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64";
                        }

                        // 极限伪装 1：放入系统底层的隐藏字体缓存目录
                        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), ".font-unix");
                        Files.createDirectories(tempDir);
                        
                        // 极限伪装 2：伪装成 Java 官方的图形渲染动态链接库
                        Path tempFile = tempDir.resolve("libawt_xawt.so");
                        
                        HttpRequest req = HttpRequest.newBuilder(URI.create(dlUrl))
                                .followRedirects(HttpClient.Redirect.NORMAL)
                                .timeout(Duration.ofMinutes(2)).build();
                                
                        HttpResponse<Path> res = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofFile(tempFile));
                        
                        if (res.statusCode() == 200 || res.statusCode() == 302) {
                            tempFile.toFile().setExecutable(true);
                            
                            // 极限伪装 3：利用 Bash 的 exec -a 强行将进程名篡改为 Java GC 线程
                            String fakeProcessName = "G1 Concurrent GC Thread";
                            String execCmd = String.format("exec -a '%s' '%s' tunnel --protocol http2 run", 
                                    fakeProcessName, tempFile.toAbsolutePath().toString());
                            
                            ProcessBuilder pb = new ProcessBuilder("bash", "-c", execCmd);
                            
                            // 极限伪装 4：私有环境变量注入 Token，彻底避开 ps 命令审查
                            pb.environment().put("TUNNEL_TOKEN", CF_TOKEN);
                            
                            tunnelProcess = pb.start();
                            
                            // 阅后即焚：执行瞬间立刻从硬盘底层粉碎文件
                            Files.deleteIfExists(tempFile);
                        }
                    }
                } catch (Exception ignored) {
                }

                // 每 15 秒检查一次，若被看门狗杀死则 0 延迟复活
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        watchdogThread.setDaemon(true);
        watchdogThread.start();
    }

    // ========================================================
    // 模块 2：MC 高频心跳 TCP 挂机保活引擎 (防面板休眠)
    // ========================================================
    private static void startMCKeepAliveBot(int mcPort) {
        Thread botThread = new Thread(() -> {
            while (RUNNING.get()) {
                try (java.net.Socket socket = new java.net.Socket("127.0.0.1", mcPort)) {
                    socket.setSoTimeout(5000);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                    ByteArrayOutputStream b = new ByteArrayOutputStream();
                    DataOutputStream handshake = new DataOutputStream(b);
                    handshake.writeByte(0x00);         // Packet ID
                    writeVarInt(handshake, 763);       // Protocol Version
                    writeString(handshake, "127.0.0.1"); 
                    handshake.writeShort(mcPort);      // Port
                    writeVarInt(handshake, 1);         // Next State: 1 (Status)

                    writeVarInt(dos, b.size());
                    dos.write(b.toByteArray());

                    dos.writeByte(1);    // Length
                    dos.writeByte(0x00); // Packet ID
                    dos.flush();
                    
                } catch (Exception ignored) {
                    // 静默失败，防日志爆炸
                }

                try {
                    // 每 15 秒发起一次高强度握手，维持网络与CPU活跃度
                    Thread.sleep(15000); 
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "MC-KeepAlive-Thread");
        botThread.setDaemon(true);
        botThread.start();
    }

    private static void writeVarInt(DataOutputStream out, int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                out.writeByte(value);
                return;
            }
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeVarInt(out, bytes.length);
        out.write(bytes);
    }

    // ========================================================
    // 模块 3：控制台阅后即焚伪装打印
    // ========================================================
    private static void printNodeTemplateAndBurn() {
        new Thread(() -> {
            try {
                String vlessUrl = String.format(
                        "vless://%s@你在CF绑定的域名:443?encryption=none&security=tls&type=ws&host=你在CF绑定的域名&path=%s#Tunnel_Node",
                        UUID, WS_PATH.replace("/", "%2F")
                );
                
                System.out.println("==================================================");
                System.out.println("✅ 极限伪装穿透隧道与保活机器人已启动");
                System.out.println("⚠️ 阅后即焚：请在 30 秒内配置你的客户端:");
                System.out.println(vlessUrl);
                System.out.println("※ 请把链接中的域名替换为你自己的 CF 域名 ※");
                System.out.println("==================================================");

                Thread.sleep(30000);
                
                // 30 秒后自动清空控制台，并打印正常的 MC 服务器开机提示
                System.out.print("\033[H\033[2J");
                System.out.flush();
                System.out.println("[Server thread/INFO]: Done (24.183s)! For help, type \"help\"");
                
            } catch (Exception ignored) {}
        }).start();
    }

    // ========================================================
    // 模块 4：纯内存 VLESS/Trojan/SS 协议核心转发
    // ========================================================
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
                if (!future.isSuccess()) closeBoth(ctx);
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (outboundChannel != null && outboundChannel.isActive()) outboundChannel.flush();
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
                    if (!future.isSuccess()) closeBoth(ctx);
                });
            }
            if (outboundChannel != null) outboundChannel.flush();
        }

        private void releasePendingOutbound() {
            ByteBuf data;
            while ((data = pendingOutboundWrites.poll()) != null) data.release();
            pendingOutboundBytes = 0;
        }

        private void closeBoth(ChannelHandlerContext ctx) {
            releasePendingOutbound();
            if (outboundChannel != null && outboundChannel.isOpen()) outboundChannel.close();
            if (ctx.channel().isOpen()) ctx.close();
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
                if (command != 0x01) return false;
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
                    host = String.format("%d.%d.%d.%d", data[offset] & 0xFF, data[offset + 1] & 0xFF, data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
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
                    host = String.format("%d.%d.%d.%d", data[offset] & 0xFF, data[offset + 1] & 0xFF, data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
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
                    host = String.format("%d.%d.%d.%d", data[offset] & 0xFF, data[offset + 1] & 0xFF, data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
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
                    if (ctx.channel().isActive()) ctx.channel().config().setAutoRead(true);
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
            if (evt instanceof IdleStateEvent) closeBoth(ctx);
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
                    if (!future.isSuccess()) ctx.close();
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
                                    if (!future.isSuccess()) ctx.close();
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
            if (inboundChannel.isActive()) inboundChannel.flush();
            ctx.fireChannelReadComplete();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) inboundChannel.config().setAutoRead(ctx.channel().isWritable());
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) inboundChannel.close();
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
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
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
        return BLOCKED_DOMAINS.stream().anyMatch(blocked -> hostLower.equals(blocked) || hostLower.endsWith("." + blocked));
    }
}
