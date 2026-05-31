package com.dripps.voxyserver.mixin;

import me.cortex.voxy.common.config.Serialization;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Redirect;

@Mixin(value = Serialization.class, remap = false)
public abstract class VoxySerializationMixin {

    @Redirect(
            method = "init",
            at = @At(value = "INVOKE", target = "Ljava/lang/Class;forName(Ljava/lang/String;)Ljava/lang/Class;")
    )
    private static Class<?> voxyserver$skipClientOnlyConfigClasses(String className) throws ClassNotFoundException {
        if (className.startsWith("me.cortex.voxy.client.config.")) {
            return Serialization.class;
        }

        return Class.forName(className);
    }
}
