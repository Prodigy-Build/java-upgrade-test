package com.github.simplenet.packet;

import com.github.simplenet.Client;
import com.github.simplenet.Server;
import com.github.simplenet.utility.Utility;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Queue;
import java.util.function.Consumer;

public final class Packet {

    private boolean prepend;
    private int size;
    private final Deque<Consumer<ByteBuffer>> stack;
    private final Deque<Consumer<ByteBuffer>> queue;

    private Packet() {
        this.queue = new ArrayDeque<>(4);
        this.stack = new ArrayDeque<>(1);
    }

    public static Packet builder() {
        return new Packet();
    }

    private Packet enqueue(Consumer<ByteBuffer> consumer) {
        if (prepend) {
            stack.push(consumer);
        } else {
            queue.offerLast(consumer);
        }

        return this;
    }

    public Packet putBoolean(boolean b) {
        size += Byte.BYTES;
        return enqueue(buffer -> buffer.put(b ? (byte) 1 : 0));
    }

    public Packet putByte(int b) {
        size += Byte.BYTES;
        return enqueue(buffer -> buffer.put((byte) b));
    }

    public Packet putBytes(byte... src) {
        size += Byte.BYTES * src.length;
        return enqueue(buffer -> buffer.put(src));
    }

    public Packet putChar(char c) {
        return putChar(c, ByteOrder.BIG_ENDIAN);
    }

    public Packet putChar(char c, ByteOrder order) {
        size += Character.BYTES;
        return enqueue(buffer -> buffer.putChar(order == ByteOrder.LITTLE_ENDIAN ? Character.reverseBytes(c) : c));
    }

    public Packet putDouble(double d) {
        return putDouble(d, ByteOrder.BIG_ENDIAN);
    }

    public Packet putDouble(double d, ByteOrder order) {
        return putLong(Double.doubleToRawLongBits(d), order);
    }

    public Packet putFloat(float f) {
        return putFloat(f, ByteOrder.BIG_ENDIAN);
    }

    public Packet putFloat(float f, ByteOrder order) {
        return putInt(Float.floatToRawIntBits(f), order);
    }

    public Packet putInt(int i) {
        return putInt(i, ByteOrder.BIG_ENDIAN);
    }

    public Packet putInt(int i, ByteOrder order) {
        size += Integer.BYTES;
        return enqueue(buffer -> buffer.putInt(order == ByteOrder.LITTLE_ENDIAN ? Integer.reverseBytes(i) : i));
    }

    public Packet putLong(long l) {
        return putLong(l, ByteOrder.BIG_ENDIAN);
    }

    public Packet putLong(long l, ByteOrder order) {
        size += Long.BYTES;
        return enqueue(buffer -> buffer.putLong(order == ByteOrder.LITTLE_ENDIAN ? Long.reverseBytes(l) : l));
    }

    public Packet putShort(int s) {
        return putShort(s, ByteOrder.BIG_ENDIAN);
    }

    public Packet putShort(int s, ByteOrder order) {
        size += Short.BYTES;
        short value = (short) s;
        return enqueue(buffer -> buffer.putShort(order == ByteOrder.LITTLE_ENDIAN ? Short.reverseBytes(value) : value));
    }

    public Packet putString(String s) {
        return putString(s, StandardCharsets.UTF_8, ByteOrder.BIG_ENDIAN);
    }

    public Packet putString(String s, Charset charset) {
        return putString(s, charset, ByteOrder.BIG_ENDIAN);
    }

    public Packet putString(String s, Charset charset, ByteOrder order) {
        var bytes = s.getBytes(charset);
        putShort(bytes.length, order);
        putBytes(bytes);
        return this;
    }

    public Packet prepend(Consumer<Packet> consumer) {
        prepend = true;
        consumer.accept(this);

        while (!stack.isEmpty()) {
            queue.offerFirst(stack.pop());
        }

        prepend = false;
        return this;
    }

    public void queue(Client client) {
        Queue<Packet> clientQueue;

        synchronized ((clientQueue = client.getOutgoingPackets())) {
            clientQueue.offer(this);
        }
    }

    public void queue(Client... clients) {
        for (Client client : clients) {
            queue(client);
        }
    }

    public void queue(Collection<? extends Client> clients) {
        clients.forEach(this::queue);
    }

    public void queueAndFlush(Client client) {
        queue(client);
        client.flush();
    }

    public void queueAndFlush(Client... clients) {
        for (Client client : clients) {
            queueAndFlush(client);
        }
    }

    public void queueAndFlush(Collection<? extends Client> clients) {
        clients.forEach(this::queueAndFlush);
    }

    public int getSize() {
        return getSize(null);
    }

    public int getSize(Client client) {
        Cipher encryptionCipher;

        if (client == null || (encryptionCipher = client.getEncryptionCipher()) == null) {
            return size;
        }

        if (!client.isEncryptionNoPadding()) {
            int blockSize = encryptionCipher.getBlockSize();
            return Utility.roundUpToNextMultiple(size, blockSize == 0 ?
                    encryptionCipher.getOutputSize(size) : blockSize);
        }

        return size;
    }

    public Deque<Consumer<ByteBuffer>> getQueue() {
        return queue;
    }
}