package com.github.simplenet.utility.exposed.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;

public interface DoubleReader extends DataReader {
    
    default void readDouble(DoubleConsumer consumer) {
        readDouble(consumer, ByteOrder.BIG_ENDIAN);
    }
    
    default void readDouble(DoubleConsumer consumer, ByteOrder order) {
        read(Double.BYTES, buffer -> consumer.accept(buffer.getDouble()), order);
    }
    
    default void readDoubleUntil(DoublePredicate predicate) {
        readDoubleUntil(predicate, ByteOrder.BIG_ENDIAN);
    }
    
    default void readDoubleUntil(DoublePredicate predicate, ByteOrder order) {
        readUntil(Double.BYTES, buffer -> predicate.test(buffer.getDouble()), order);
    }
    
    default void readDoubleAlways(DoubleConsumer consumer) {
        readDoubleAlways(consumer, ByteOrder.BIG_ENDIAN);
    }
    
    default void readDoubleAlways(DoubleConsumer consumer, ByteOrder order) {
        readAlways(Double.BYTES, buffer -> consumer.accept(buffer.getDouble()), order);
    }
    
    default void readDoubles(int n, Consumer<double[]> consumer) {
        readDoubles(n, consumer, ByteOrder.BIG_ENDIAN);
    }
    
    default void readDoubles(int n, Consumer<double[]> consumer, ByteOrder order) {
        read(Double.BYTES * n, buffer -> processDoubles(buffer, n, consumer), order);
    }
    
    default void readDoublesAlways(int n, Consumer<double[]> consumer) {
        readDoublesAlways(n, consumer, ByteOrder.BIG_ENDIAN);
    }
    
    default void readDoublesAlways(int n, Consumer<double[]> consumer, ByteOrder order) {
        readAlways(Double.BYTES * n, buffer -> processDoubles(buffer, n, consumer), order);
    }
    
    private void processDoubles(ByteBuffer buffer, int n, Consumer<double[]> consumer) {
        double[] d = new double[n];
        buffer.asDoubleBuffer().get(d);
        consumer.accept(d);
    }
}