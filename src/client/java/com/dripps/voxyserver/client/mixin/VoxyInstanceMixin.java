package com.dripps.voxyserver.client.mixin;

import com.dripps.voxyserver.client.service.IVoxyServerIngestAccess;
import com.dripps.voxyserver.client.service.RemoteIngestService;
import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.thread.ServiceManager;
import me.cortex.voxy.commonImpl.VoxyInstance;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(VoxyInstance.class)
public abstract class VoxyInstanceMixin implements IVoxyServerIngestAccess {

    @Unique
    private RemoteIngestService voxyserver$remoteIngestService;

    @Unique
    private boolean voxyserver$usingRemoteIngest = false;

    @Shadow
    public abstract ServiceManager getServiceManager();

    @Inject(method = "<init>()V", at = @At("RETURN"))
    private void voxyserver$init(CallbackInfo ci) {
        Logger.info("startuig RemoteIngestService");
        this.voxyserver$remoteIngestService = new RemoteIngestService(this.getServiceManager());
    }


    @Inject(
            method = "shutdown()V",
            at = @At(
                    value = "INVOKE",
                    target = "Lme/cortex/voxy/common/world/service/VoxelIngestService;shutdown()V"
            )
    )
    private void voxyserver$shutdown(CallbackInfo ci) {
        Logger.info("shutting down RemoteIngestService");
        try {
            this.voxyserver$remoteIngestService.shutdown();
        } catch (Exception e) {
            Logger.error("error shutting down RemoteIngestService", e);
        }
    }


    @Inject(
            method = "isIngestEnabled(Lme/cortex/voxy/commonImpl/WorldIdentifier;)Z",
            at = @At("HEAD"),
            cancellable = true
    )
    private void voxyserver$disableClientSideIngest(WorldIdentifier worldId, CallbackInfoReturnable<Boolean> cir) {
        if (this.voxyserver$usingRemoteIngest) {
            cir.setReturnValue(false);
        }
    }

    @Override
    public RemoteIngestService voxyserver$getRemoteIngestService() {
        return this.voxyserver$remoteIngestService;
    }

    @Override
    public void voxyserver$setUsingRemoteIngest(boolean remoteIngest) {
        this.voxyserver$usingRemoteIngest = remoteIngest;
    }
}