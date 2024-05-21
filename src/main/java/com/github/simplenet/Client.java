package com.github.simplenet;

import com.github.pbbl.AbstractBufferPool;
import com.github.pbbl.direct.DirectByteBufferPool;
import com.github.simplenet.packet.Packet;
import com.github.simplenet.utility.IntPair;
import com.github.simplenet.utility.MutableBoolean;
import com.github.simplenet.utility.Pair;
import com.github.simplenet.utility.Utility;
import com.github.simplenet.utility.exposed.cryptography.CryptographicFunction;
import com.github.simplenet.utility.exposed.data.BooleanReader;
import com.github.simplenet.utility.exposed.data.ByteReader;
import com.github.simplenet.utility.exposed.data.CharReader;
import com.github.simplenet.utility.exposed.data.DoubleReader;
import com.github.simplenet.utility.exposed.data.FloatReader;
import com.github.simplenet.utility.exposed.data.IntReader;
import com.github.simplenet.utility.exposed.data.LongReader;
import com.github.simplenet.utility.exposed.data.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class Client extends AbstractReceiver<Runnable> implements Channeled<AsynchronousSocketChannel>, BooleanReader,
        ByteReader, CharReader, IntReader, FloatReader, LongReader, DoubleReader, StringReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    static class Listener implements CompletionHandler<Integer, Pair<Client, ByteBuffer>> {

        static final Listener INSTANCE = new Listener();
        
        @Override
        public void completed(Integer result, Pair<Client, ByteBuffer> pair) {
            int bytesReceived = result;

            if (bytesReceived == -1) {
                pair.getKey().close(false);
                return;
            }

            var client = pair.getKey();
            var buffer = pair.getValue().flip();

            synchronized (client.queue) {
                var queue = client.queue;

                IntPair<Predicate<ByteBuffer>> peek;

                if ((peek = queue.peekLast()) == null) {
                    client.readInProgress.set(false);
                    return;
                }

                var stack = client.stack;

                boolean shouldDecrypt = client.decryptionCipher != null;
                boolean queueIsEmpty = false;

                int key;

                client.inCallback.set(true);

                while (buffer.remaining() >= (key = peek.getKey())) {
                    var wrappedBuffer = buffer.duplicate().mark().limit(buffer.position() + key);

                    if (shouldDecrypt) {
                        try {
                            wrappedBuffer = client.decryptionFunction.apply(client.decryptionCipher, wrappedBuffer)
                                    .reset();
                        } catch (Exception e) {
                            throw new IllegalStateException("An exception occurred whilst encrypting data:", e);
                        }
                    }

                    if (!peek.getValue().test(wrappedBuffer)) {
                        queue.pollLast();
                    }

                    if (wrappedBuffer.hasRemaining()) {
                        int remaining = wrappedBuffer.remaining();
                        byte[] decodedData = new byte[Math.min(key, 8)];
                        wrappedBuffer.reset().get(decodedData);
                        LOGGER.warn("A packet has not been read fully! {} byte(s) leftover! First 8 bytes of data: {}",
                                remaining, decodedData);
                    }

                    buffer.position(wrappedBuffer.limit());

                    while (!stack.isEmpty()) {
                        queue.offerLast(stack.pop());
                    }

                    if ((peek = queue.peekLast()) == null) {
                        queueIsEmpty = true;
                        break;
                    }
                }

                client.inCallback.set(false);

                if (!queueIsEmpty && buffer.hasRemaining()) {
                    client.channel.read(buffer.position(buffer.limit()).limit(key), pair, this);
                } else {
                    DIRECT_BUFFER_POOL.give(buffer);

                    if (queueIsEmpty) {
                        client.readInProgress.set(false);
                    } else {
                        var newBuffer = DIRECT_BUFFER_POOL.take(peek.getKey());
                        client.channel.read(newBuffer, new Pair<>(client, newBuffer), this);
                    }
                }
            }
        }

        @Override
        public void failed(Throwable t, Pair<Client, ByteBuffer> pair) {
            pair.getKey().close(false);
        }
    }

    private final CompletionHandler<Integer, ByteBuffer> packetHandler = new CompletionHandler<>() {
        @Override
        public void completed(Integer result, ByteBuffer buffer) {
            Client client = Client.this;
    
            DIRECT_BUFFER_POOL.give(buffer);

            synchronized (client.outgoingPackets) {
                ByteBuffer payload = client.packetsToFlush.poll();
    
                if (payload == null) {
                    client.writeInProgress.set(false);
                    return;
                }

                client.channel.write(payload, payload, this);
            }
        }

        @Override
        public void failed(Throwable t, ByteBuffer buffer) {
            Client client = Client.this;

            DIRECT_BUFFER_POOL.give(buffer);

            synchronized (client.outgoingPackets) {
                ByteBuffer discard;

                while ((discard = client.packetsToFlush.poll()) != null) {
                    DIRECT_BUFFER_POOL.give(discard);
                }
            }

            client.writeInProgress.set(false);
        }
    };

    private static final AbstractBufferPool<ByteBuffer> DIRECT_BUFFER_POOL = new DirectByteBufferPool();

    private final MutableBoolean inCallback;
    
    private final AtomicBoolean closing;
    
    private final AtomicBoolean readInProgress;
    
    private final AtomicBoolean writeInProgress;
    
    private final Queue<Packet> outgoingPackets;

    private final Queue<ByteBuffer> packetsToFlush;

    private final Deque<IntPair<Predicate<ByteBuffer>>> stack;

    private final Deque<IntPair<Predicate<ByteBuffer>>> queue;

    private boolean decryptionNoPadding;

    private boolean encryptionNoPadding;

    private Cipher decryptionCipher;

    private Cipher encryptionCipher;

    private CryptographicFunction decryptionFunction;

    private CryptographicFunction encryptionFunction;

    private AsynchronousChannelGroup group;
    
    private AsynchronousSocketChannel channel;
    
    public Client() {
        this((AsynchronousSocketChannel) null);
    }

    Client(AsynchronousSocketChannel channel) {
        closing = new AtomicBoolean();
        inCallback = new MutableBoolean();
        readInProgress = new AtomicBoolean();
        writeInProgress = new AtomicBoolean();
        outgoingPackets = new ArrayDeque<>();
        packetsToFlush = new ArrayDeque<>();
        queue = new ArrayDeque<>();
        stack = new ArrayDeque<>();
        
        if (channel != null) {
            this.channel = channel;
        }
    }
    
    protected Client(Client client) {
        super(client);

        this.stack = client.stack;
        this.queue = client.queue;
        this.channel = client.channel;
        this.closing = client.closing;
        this.inCallback = client.inCallback;
        this.packetsToFlush = client.packetsToFlush;
        this.readInProgress = client.readInProgress;
        this.writeInProgress = client.writeInProgress;
        this.outgoingPackets = client.outgoingPackets;
        this.encryptionCipher = client.encryptionCipher;
        this.decryptionCipher = client.decryptionCipher;
        this.encryptionFunction = client.encryptionFunction;
        this.decryptionFunction = client.decryptionFunction;
        this.decryptionNoPadding = client.decryptionNoPadding;
    }

    public final void connect(String address, int port) {
        connect(address, port, 30L, TimeUnit.SECONDS, () ->
            LOGGER.warn("Couldn't connect to the server! Maybe it's offline?"));
    }

    public final void connect(String address, int port, long timeout, TimeUnit unit, Runnable onTimeout) {
        Objects.requireNonNull(address);

        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("The specified port must be between 0 and 65535!");
        }

        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(false);
            thread.setName(thread.getName().replace("Thread", "SimpleNet"));
            
            return thread;
        }, (runnable, threadPoolExecutor) -> {});

        executor.prestartCoreThread();

        try {
            this.channel = AsynchronousSocketChannel.open(group = AsynchronousChannelGroup.withThreadPool(executor));
            this.channel.setOption(StandardSocketOptions.SO_RCVBUF, BUFFER_SIZE);
            this.channel.setOption(StandardSocketOptions.SO_SNDBUF, BUFFER_SIZE);
            this.channel.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
            this.channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to open the channel!", e);
        }

        try {
            channel.connect(new InetSocketAddress(address, port)).get(timeout, unit);
        } catch (AlreadyConnectedException e) {
            throw new IllegalStateException("This client is already connected to a server!", e);
        } catch (Exception e) {
            onTimeout.run();
            close(false);
            return;
        }
        
        executor.execute(() -> connectListeners.forEach(Runnable::run));
    }

    private void close(boolean waitForWrite) {
        if (closing.getAndSet(true)) {
            return;
        }

        preDisconnectListeners.forEach(Runnable::run);

        if (waitForWrite) {
            flush();

            while (writeInProgress.get()) {
                Thread.onSpinWait();
            }
        }

        Channeled.super.close();

        while (channel.isOpen()) {
            Thread.onSpinWait();
        }

        postDisconnectListeners.forEach(Runnable::run);

        if (group != null) {
            try {
                group.shutdownNow();
            } catch (IOException e) {
                LOGGER.debug("An IOException occurred when shutting down the AsynchronousChannelGroup!", e);
            }
        }
    }

    @Override
    public final void close() {
        close(true);
    }

    public final void preDisconnect(Runnable listener) {
        preDisconnectListeners.add(listener);
    }

    public final void postDisconnect(Runnable listener) {
        postDisconnectListeners.add(listener);
    }
    
    @Override
    public void readUntil(int n, Predicate<ByteBuffer> predicate, ByteOrder order) {
        boolean shouldDecrypt = decryptionCipher != null;
    
        if (shouldDecrypt && !decryptionNoPadding) {
            int blockSize = decryptionCipher.getBlockSize();
            n = Utility.roundUpToNextMultiple(n, blockSize == 0 ? decryptionCipher.getOutputSize(n) : blockSize);
        }

        var pair = new IntPair<Predicate<ByteBuffer>>(n, buffer -> predicate.test(buffer.order(order)));

        synchronized (queue) {
            if (inCallback.get()) {
                stack.push(pair);
                return;
            }

            queue.offerFirst(pair);

            if (!readInProgress.getAndSet(true)) {
                var buffer = DIRECT_BUFFER_POOL.take(n);
                channel.read(buffer, new Pair<>(this, buffer), Listener.INSTANCE);
            }
        }
    }

    public final void flush() {
        Packet packet;

        boolean shouldEncrypt = encryptionCipher != null;

        Deque<Consumer<ByteBuffer>> queue;

        synchronized (outgoingPackets) {
            while ((packet = outgoingPackets.poll()) != null) {
                queue = packet.getQueue();

                ByteBuffer raw = DIRECT_BUFFER_POOL.take(packet.getSize(this));

                for (var input : queue) {
                    input.accept(raw);
                }

                if (shouldEncrypt) {
                    try {
                        raw = encryptionFunction.apply(encryptionCipher, raw.flip());
                    } catch (GeneralSecurityException e) {
                        throw new IllegalStateException("An exception occurred whilst encrypting data!", e);
                    }
                }

                raw.flip();

                if (!writeInProgress.getAndSet(true)) {
                    channel.write(raw, raw, packetHandler);
                } else {
                    packetsToFlush.offer(raw);
                }
            }
        }
    }

    public final Queue<Packet> getOutgoingPackets() {
        return outgoingPackets;
    }

    @Override
    public final AsynchronousSocketChannel getChannel() {
        return channel;
    }
    
    public final Cipher getEncryptionCipher() {
        return encryptionCipher;
    }
    
    public final Cipher getDecryptionCipher() {
        return decryptionCipher;
    }
    
    public final void setEncryptionCipher(Cipher encryptionCipher) {
        setEncryption(encryptionCipher, CryptographicFunction.DO_FINAL);
    }
    
    public final void setEncryption(Cipher encryptionCipher, CryptographicFunction encryptionFunction) {
        this.encryptionCipher = encryptionCipher;
        this.encryptionFunction = encryptionFunction;
        this.encryptionNoPadding = encryptionCipher.getAlgorithm().endsWith("NoPadding");
    }

    public boolean isEncryptionNoPadding() {
        return encryptionNoPadding;
    }

    public final void setDecryptionCipher(Cipher decryptionCipher) {
        setDecryption(decryptionCipher, CryptographicFunction.DO_FINAL);
    }
    
    public final void setDecryption(Cipher decryptionCipher, CryptographicFunction decryptionFunction) {
        this.decryptionCipher = decryptionCipher;
        this.decryptionFunction = decryptionFunction;
        this.decryptionNoPadding = decryptionCipher.getAlgorithm().endsWith("NoPadding");
    }
}