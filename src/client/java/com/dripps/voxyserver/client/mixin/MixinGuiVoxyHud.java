package com.dripps.voxyserver.client.mixin;

import com.dripps.voxyserver.client.VoxyDownloadHud;
import net.minecraft.client.DeltaTracker;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.Gui;
import net.minecraft.client.gui.GuiGraphicsExtractor;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

// Draws the non-blocking VoxyServer download overlay on top of the vanilla HUD.
// extractRenderState runs every frame during gameplay and does not consume input,
// so the game stays fully playable.
@Mixin(Gui.class)
public class MixinGuiVoxyHud {

    @Inject(method = "extractRenderState", at = @At("TAIL"))
    private void voxyserver$downloadHud(GuiGraphicsExtractor graphics, DeltaTracker deltaTracker, CallbackInfo ci) {
        VoxyDownloadHud.render(graphics, Minecraft.getInstance().font);
    }
}
