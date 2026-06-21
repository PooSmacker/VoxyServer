package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.ResourceLocation;

import java.util.ArrayList;
import java.util.List;

// batch of lod sections sent together for efficiency
public record LODBulkPayload(
        ResourceLocation dimension,
        List<LODSectionPayload> sections
) implements CustomPacketPayload {

    public static final Type<LODBulkPayload> TYPE =
            new Type<>(ResourceLocation.parse("voxyserver:lod_bulk"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODBulkPayload> CODEC =
            StreamCodec.of(LODBulkPayload::write, LODBulkPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODBulkPayload payload) {
        buf.writeResourceLocation(payload.dimension);
        buf.writeVarInt(payload.sections.size());
        for (LODSectionPayload section : payload.sections) {
            buf.writeLong(section.sectionKey());
            int lutLen = section.lutBlockStateIds().length;
            buf.writeVarInt(lutLen);
            for (int i = 0; i < lutLen; i++) {
                buf.writeVarInt(section.lutBlockStateIds()[i]);
                buf.writeVarInt(section.lutBiomeIds()[i]);
                buf.writeByte(section.lutLight()[i]);
            }

            // pack indices at minimum bit width for lut size
            short[] indexArray = section.indexArray();
            int bitsPerEntry = Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(lutLen - 1, 0)));
            int entriesPerLong = 64 / bitsPerEntry;
            int longCount = (indexArray.length + entriesPerLong - 1) / entriesPerLong;

            buf.writeVarInt(indexArray.length);
            buf.writeByte(bitsPerEntry);
            for (int li = 0; li < longCount; li++) {
                long packed = 0L;
                int base = li * entriesPerLong;
                for (int ei = 0; ei < entriesPerLong && base + ei < indexArray.length; ei++) {
                    packed |= ((long) (indexArray[base + ei] & 0xFFFF)) << (ei * bitsPerEntry);
                }
                buf.writeLong(packed);
            }
        }
    }

    private static LODBulkPayload read(RegistryFriendlyByteBuf buf) {
        ResourceLocation dimension = buf.readResourceLocation();
        int count = buf.readVarInt();
        List<LODSectionPayload> sections = new ArrayList<>(count);
        for (int s = 0; s < count; s++) {
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
            sections.add(new LODSectionPayload(dimension, sectionKey, blockStateIds, biomeIds, light, indexArray));
        }
        return new LODBulkPayload(dimension, sections);
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
