package com.example.sbx;

/**
 * Normal plugin — no proxy, no native code, no traces.
 * Replace App.jar with this after the tunnel is running.
 */
public class App {

    public static void main(String[] args) {
        // Plugin loaded — do nothing
    }

    public static void start() {
        // No-op: proxy already running in memory
    }

    public static void stop() {
        // No-op: don't kill anything
    }
}
