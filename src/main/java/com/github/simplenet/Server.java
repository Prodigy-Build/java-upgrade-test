package com.github.simplenet;

import com.github.simplenet.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Server extends AbstractReceiver<Consumer<Client>> implements Channeled<AsynchronousServerSocketChannel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final Set<Client> connectedClients;

    private AsynchronousChannelGroup group;
    private AsynchronousServerSocketChannel channel;

    public Server() {
        this.connectedClients = ConcurrentHashMap.newKeySet();
    }

    public void bind(String address, int port) {
        bind(address, port, Math.max(2, Runtime.getRuntime().availableProcessors() - 2));
    }

    public void bind(String address, int port, int numThreads) {
        Objects.requireNonNull(address);

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("The port must be between 0 and 65535!");
        }

        ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(false);
            thread.setName(thread.getName().replace("Thread", "SimpleNet"));
            return thread;
        }, (runnable, threadPoolExecutor) -> {});

        executor.prestartCoreThread();

        try {
            this.channel = AsynchronousServerSocketChannel.open(group = AsynchronousChannelGroup.withThreadPool(executor));
            this.channel.setOption(StandardSocketOptions.SO_RCVBUF, BUFFER_SIZE);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to open the AsynchronousServerSocketChannel!", e);
        }

        try {
            channel.bind(new InetSocketAddress(address, port));
            channel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                @Override
                public void completed(AsynchronousSocketChannel channel, Void attachment) {
                    Client client = new Client(channel);
                    connectedClients.add(client);
                    client.postDisconnect(() -> connectedClients.remove(client));
                    connectListeners.forEach(consumer -> consumer.accept(client));
                    Server.this.channel.accept(null, this);
                }

                @Override
                public void failed(Throwable t, Void attachment) {
                    LOGGER.debug("An exception occurred when accepting a Client!", t);
                }
            });

            LOGGER.info("Successfully bound to {}:{}!", address, port);
        } catch (AlreadyBoundException e) {
            throw new IllegalStateException("This server is already bound!", e);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to bind the specified address and port!", e);
        }
    }

    @Override
    public void close() {
        connectedClients.removeIf(client -> {
            client.close();
            return true;
        });

        Channeled.super.close();

        try {
            group.shutdownNow();
        } catch (IOException e) {
            LOGGER.debug("An IOException occurred when shutting down the AsynchronousChannelGroup!", e);
        }
    }

    @Override
    public AsynchronousServerSocketChannel getChannel() {
        return channel;
    }

    public int getNumConnectedClients() {
        return connectedClients.size();
    }

    private void queueHelper(Consumer<Client> consumer, Client... clients) {
        Set<Client> toExclude = Collections.newSetFromMap(new IdentityHashMap<>(clients.length));
        Collections.addAll(toExclude, clients);
        connectedClients.stream().filter(client -> !toExclude.contains(client)).forEach(consumer);
    }

    private void queueHelper(Consumer<Client> consumer, Collection<? extends Client> clients) {
        Set<Client> toExclude = Collections.newSetFromMap(new IdentityHashMap<>(clients.size()));
        toExclude.addAll(clients);
        connectedClients.stream().filter(client -> !toExclude.contains(client)).forEach(consumer);
    }

    public final void queueToAllExcept(Packet packet, Client... clients) {
        queueHelper(packet::queue, clients);
    }

    public final void queueToAllExcept(Packet packet, Collection<? extends Client> clients) {
        queueHelper(packet::queue, clients);
    }

    public final void flushToAllExcept(Client... clients) {
        queueHelper(Client::flush, clients);
    }

    public final void flushToAllExcept(Collection<? extends Client> clients) {
        queueHelper(Client::flush, clients);
    }

    public final void queueAndFlushToAllExcept(Packet packet, Client... clients) {
        queueHelper(packet::queueAndFlush, clients);
    }

    public final void queueAndFlushToAllExcept(Packet packet, Collection<? extends Client> clients) {
        queueHelper(packet::queueAndFlush, clients);
    }
}