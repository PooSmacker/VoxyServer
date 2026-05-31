package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.ResourceLocation;

// single level 0 LOD section sent from server to client
// contains: dimension id, section position key, a LUT of vanilla registry ids, and an index array
public record LODSectionPayload(
        ResourceLocation dimension,
        long sectionKey,
        int[] lutBlockStateIds,
        int[] lutBiomeIds,
        byte[] lutLight,
        short[] indexArray
) implements CustomPacketPayload {

    public static final Type<LODSectionPayload> TYPE =
            new Type<>(ResourceLocation.parse("voxyserver:lod_section"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODSectionPayload> CODEC =
            StreamCodec.of(LODSectionPayload::write, LODSectionPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODSectionPayload payload) {
        buf.writeResourceLocation(payload.dimension);
        buf.writeLong(payload.sectionKey);

        int lutLen = payload.lutBlockStateIds.length;
        buf.writeVarInt(lutLen);
        for (int i = 0; i < lutLen; i++) {
            buf.writeVarInt(payload.lutBlockStateIds[i]);
            buf.writeVarInt(payload.lutBiomeIds[i]);
            buf.writeByte(payload.lutLight[i]);
        }

        // pack indices at minimum bit width for lut size
        int bitsPerEntry = Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(lutLen - 1, 0)));
        int entriesPerLong = 64 / bitsPerEntry;
        int longCount = (payload.indexArray.length + entriesPerLong - 1) / entriesPerLong;

        buf.writeVarInt(payload.indexArray.length);
        buf.writeByte(bitsPerEntry);
        for (int li = 0; li < longCount; li++) {
            long packed = 0L;
            int base = li * entriesPerLong;
            for (int ei = 0; ei < entriesPerLong && base + ei < payload.indexArray.length; ei++) {
                packed |= ((long) (payload.indexArray[base + ei] & 0xFFFF)) << (ei * bitsPerEntry);
            }
            buf.writeLong(packed);
        }
    }

    private static LODSectionPayload read(RegistryFriendlyByteBuf buf) {
        ResourceLocation dimension = buf.readResourceLocation();
        long sectionKey = buf.readLong();

        int lutLen = buf.readVarInt();
        int[] blockStateIds = new int[lutLen];
        int[] biomeIds = new int[lutLen];
        byte[] light = new byte[lutLen];
        for (int i = 0; i < lutLen; i++) {
            blockStateIds[i] = buf.readVarInt();
            biomeIds[i] = buf.readVarInt();
            light[i] = buf.readByte();
        }

        int indexLen = buf.readVarInt();
        int bitsPerEntry = buf.readByte() & 0xFF;
        int entriesPerLong = 64 / bitsPerEntry;
        long mask = (1L << bitsPerEntry) - 1;
        short[] indexArray = new short[indexLen];
        int idx = 0;
        while (idx < indexLen) {
            long packed = buf.readLong();
            for (int ei = 0; ei < entriesPerLong && idx < indexLen; ei++, idx++) {
                indexArray[idx] = (short) ((packed >> (ei * bitsPerEntry)) & mask);
            }
        }

        return new LODSectionPayload(dimension, sectionKey, blockStateIds, biomeIds, light, indexArray);
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
