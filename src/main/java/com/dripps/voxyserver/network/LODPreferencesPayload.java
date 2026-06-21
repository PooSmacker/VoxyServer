package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.ResourceLocation;

public record LODPreferencesPayload(
        boolean enabled,
        int lodStreamRadius,
        int maxSectionsPerTick
) implements CustomPacketPayload {

    public static final Type<LODPreferencesPayload> TYPE =
            new Type<>(ResourceLocation.parse("voxyserver:lod_preferences"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODPreferencesPayload> CODEC =
            StreamCodec.of(LODPreferencesPayload::write, LODPreferencesPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODPreferencesPayload payload) {
        buf.writeBoolean(payload.enabled);
        buf.writeVarInt(payload.lodStreamRadius);
        buf.writeVarInt(payload.maxSectionsPerTick);
    }

    private static LODPreferencesPayload read(RegistryFriendlyByteBuf buf) {
        boolean enabled = buf.readBoolean();
        int lodStreamRadius = buf.readVarInt();
        int maxSectionsPerTick = buf.readVarInt();
        return new LODPreferencesPayload(enabled, lodStreamRadius, maxSectionsPerTick);
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
