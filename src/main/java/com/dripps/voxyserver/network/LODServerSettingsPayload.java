package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.ResourceLocation;

public record LODServerSettingsPayload(
        int maxLodStreamRadius,
        int maxSectionsPerTick
) implements CustomPacketPayload {

    public static final Type<LODServerSettingsPayload> TYPE =
            new Type<>(ResourceLocation.parse("voxyserver:lod_server_settings"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODServerSettingsPayload> CODEC =
            StreamCodec.of(LODServerSettingsPayload::write, LODServerSettingsPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODServerSettingsPayload payload) {
        buf.writeVarInt(payload.maxLodStreamRadius);
        buf.writeVarInt(payload.maxSectionsPerTick);
    }

    private static LODServerSettingsPayload read(RegistryFriendlyByteBuf buf) {
        int maxLodStreamRadius = buf.readVarInt();
        int maxSectionsPerTick = buf.readVarInt();
        return new LODServerSettingsPayload(maxLodStreamRadius, maxSectionsPerTick);
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
