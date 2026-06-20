package com.dripps.voxyserver.client;

import net.minecraft.client.gui.Font;
import net.minecraft.client.gui.GuiGraphicsExtractor;
import net.minecraft.network.chat.Component;

// Non-blocking HUD overlay shown while the server streams LOD terrain. A few lines
// centered (or top-left) at the top of the screen; the game stays fully playable and
// interactive (no screen, no buttons). Auto-hides when the transfer completes or goes
// stale, and respects the "Show Download HUD" client setting.
public final class VoxyDownloadHud {
    private static final int COLOR = 0xFFFFFFFF;
    private static final int BAR_CELLS = 24;
    private static final int LINE_SPACING = 11;
    private static final int TOP_MARGIN = 4;
    private static final int LEFT_MARGIN = 4;
    private static final long STALE_MS = 10_000L;

    private static volatile int sent;
    private static volatile int total;
    private static volatile boolean active;
    private static volatile long lastUpdateMs;

    private VoxyDownloadHud() {}

    // fed from the LODProgressPayload receiver
    public static void onProgress(int sent, int total, boolean complete) {
        if (complete) {
            active = false;
            return;
        }
        if (total <= 0 && sent <= 0) {
            return;
        }
        VoxyDownloadHud.sent = sent;
        VoxyDownloadHud.total = total;
        VoxyDownloadHud.lastUpdateMs = System.currentTimeMillis();
        VoxyDownloadHud.active = true;
    }

    public static void reset() {
        active = false;
    }

    private static boolean visible() {
        if (!active) return false;
        if (!ClientLodSettings.isDownloadHudEnabled()) return false;
        if (System.currentTimeMillis() - lastUpdateMs > STALE_MS) {
            active = false;
            return false;
        }
        return true;
    }

    // called every frame from the Gui extract-render-state mixin
    public static void render(GuiGraphicsExtractor g, Font font) {
        if (!visible()) return;

        int w = g.guiWidth();
        boolean topLeft = ClientLodSettings.isDownloadHudTopLeft();
        int s = sent;
        int t = total;

        String line1 = "Downloading world data";
        String line2;
        String line3;
        if (t > 0) {
            int pct = (int) Math.min(100L, Math.max(0L, (long) s * 100L / t));
            int fill = (int) Math.min(BAR_CELLS, (long) BAR_CELLS * s / t);
            StringBuilder bar = new StringBuilder("[");
            for (int i = 0; i < BAR_CELLS; i++) {
                bar.append(i < fill ? '█' : '░');
            }
            bar.append("] ").append(pct).append('%');
            line2 = bar.toString();
            line3 = String.format("%,d / %,d LOD sections", Math.min(s, t), t);
        } else {
            line2 = "preparing…";
            line3 = String.format("%,d LOD sections", s);
        }

        int y = TOP_MARGIN;
        drawLine(g, font, line1, w, y, topLeft);
        y += LINE_SPACING;
        drawLine(g, font, line2, w, y, topLeft);
        y += LINE_SPACING;
        drawLine(g, font, line3, w, y, topLeft);
    }

    private static void drawLine(GuiGraphicsExtractor g, Font font, String text, int width, int y, boolean topLeft) {
        Component c = Component.literal(text);
        int x = topLeft ? LEFT_MARGIN : (width - font.width(c)) / 2;
        g.text(font, c, x, y, COLOR, true);
    }
}
