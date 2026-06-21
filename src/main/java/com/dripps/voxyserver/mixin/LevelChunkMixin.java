package com.dripps.voxyserver.mixin;

import com.dripps.voxyserver.server.DirtyTracker;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(LevelChunk.class)
public abstract class LevelChunkMixin {

    @Shadow
    public abstract Level getLevel();

    @Inject(method = "setBlockState", at = @At("RETURN"))
    private void voxyserver$onBlockChanged(BlockPos pos, BlockState state, boolean moved, CallbackInfoReturnable<BlockState> cir) {
        if (cir.getReturnValue() == null) return; // no actual change
        Level level = this.getLevel();
        if (!(level instanceof ServerLevel serverLevel)) return;

        DirtyTracker tracker = DirtyTracker.INSTANCE;
        if (tracker != null) {
            LevelChunk self = (LevelChunk) (Object) this;
            tracker.markDirty(serverLevel, self.getPos().x, pos.getY(), self.getPos().z);
        }
    }
}
