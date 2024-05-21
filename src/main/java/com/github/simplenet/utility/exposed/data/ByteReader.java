package com.github.simplenet.utility.exposed.data;

import com.github.simplenet.utility.exposed.consumer.ByteConsumer;
import com.github.simplenet.utility.exposed.predicate.BytePredicate;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

public interface ByteReader extends DataReader {
    
    default void readByte(ByteConsumer consumer) {
        read(Byte.BYTES, buffer -> consumer.accept(buffer.get()), ByteOrder.BIG_ENDIAN);
    }
    
    default void readByteUntil(BytePredicate predicate) {
        readUntil(Byte.BYTES, buffer -> predicate.test(buffer.get()), ByteOrder.BIG_ENDIAN);
    }
    
    default void readByteAlways(ByteConsumer consumer) {
        readAlways(Byte.BYTES, buffer -> consumer.accept(buffer.get()), ByteOrder.BIG_ENDIAN);
    }
    
    default void readBytes(int n, Consumer<byte[]> consumer) {
        read(Byte.BYTES * n, buffer -> processBytes(buffer, n, consumer), ByteOrder.BIG_ENDIAN);
    }
    
    default void readBytesAlways(int n, Consumer<byte[]> consumer) {
        readAlways(Byte.BYTES * n, buffer -> processBytes(buffer, n, consumer), ByteOrder.BIG_ENDIAN);
    }
    
    private void processBytes(ByteBuffer buffer, int n, Consumer<byte[]> consumer) {
        var b = new byte[n];
        buffer.get(b);
        consumer.accept(b);
    }
}