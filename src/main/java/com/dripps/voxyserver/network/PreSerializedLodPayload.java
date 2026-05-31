package com.dripps.voxyserver.network;

import io.netty.buffer.Unpooled;
import net.minecraft.core.RegistryAccess;
import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.ResourceLocation;

// wraps preserialized LODBulkPayload bytes so the heavy encoding
// happens on the streaming thread instead of the tick thread
public record PreSerializedLodPayload(byte[] data) implements CustomPacketPayload {

    public static final Type<PreSerializedLodPayload> TYPE =
            new Type<>(ResourceLocation.parse("voxyserver:lod_preserialized"));

    public static final StreamCodec<RegistryFriendlyByteBuf, PreSerializedLodPayload> CODEC =
            StreamCodec.of(PreSerializedLodPayload::write, PreSerializedLodPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, PreSerializedLodPayload payload) {
        buf.writeVarInt(payload.data.length);
        buf.writeBytes(payload.data);
    }

    private static PreSerializedLodPayload read(RegistryFriendlyByteBuf buf) {
        int len = buf.readVarInt();
        byte[] data = new byte[len];
        buf.readBytes(data);
        return new PreSerializedLodPayload(data);
    }

    // encodes an LODBulkPayload to preserialized bytes on the calling thread
    public static PreSerializedLodPayload fromBulk(LODBulkPayload bulk, RegistryAccess registryAccess) {
        var raw = Unpooled.buffer();
        try {
            var rfb = new RegistryFriendlyByteBuf(raw, registryAccess);
            LODBulkPayload.CODEC.encode(rfb, bulk);
            byte[] bytes = new byte[rfb.readableBytes()];
            rfb.readBytes(bytes);
            return new PreSerializedLodPayload(bytes);
        } finally {
            raw.release();
        }
    }


    public LODBulkPayload decodeBulk(RegistryAccess registryAccess) {
        var raw = Unpooled.wrappedBuffer(data);
        try {
            var rfb = new RegistryFriendlyByteBuf(raw, registryAccess);
            return LODBulkPayload.CODEC.decode(rfb);
        } finally {
            raw.release();
        }
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
