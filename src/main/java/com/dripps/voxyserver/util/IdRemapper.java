package com.dripps.voxyserver.util;

import me.cortex.voxy.common.world.other.Mapper;
import net.minecraft.core.Registry;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;

public class IdRemapper {

    // get vanilla block state registry id from a voxy mapper long id
    public static int toVanillaBlockStateId(Mapper mapper, long mappingId) {
        int blockId = Mapper.getBlockId(mappingId);
        if (blockId == 0) return 0; // air
        BlockState state = mapper.getBlockStateFromBlockId(blockId);
        return Block.BLOCK_STATE_REGISTRY.getId(state);
    }

    // get vanilla biome registry id from a voxy mapper long id
    public static int toVanillaBiomeId(ServerLevel level, Mapper mapper, long mappingId) {
        Registry<Biome> biomeRegistry = level.registryAccess().registryOrThrow(Registries.BIOME);
        return toVanillaBiomeIdFromMapper(mapper, mappingId, biomeRegistry);
    }

    // version that takes a pre fetched reg, safe to call offthread
    public static int toVanillaBiomeIdFromMapper(Mapper mapper, long mappingId) {
        // return raw biome id when no registry available, this is a fallback
        return Mapper.getBiomeId(mappingId);
    }

    public static int toVanillaBiomeIdFromMapper(Mapper mapper, long mappingId, Registry<Biome> biomeRegistry) {
        int biomeId = Mapper.getBiomeId(mappingId);
        Mapper.BiomeEntry[] entries = mapper.getBiomeEntries();
        if (biomeId >= entries.length) return 0;
        String biomeName = entries[biomeId].biome;
        if (biomeName == null) return 0;
        ResourceLocation biomeKey = ResourceLocation.parse(biomeName);
        return biomeRegistry.getId(biomeRegistry.get(biomeKey));
    }

    public static int getLightFromMapping(long mappingId) {
        return Mapper.getLightId(mappingId);
    }
}
