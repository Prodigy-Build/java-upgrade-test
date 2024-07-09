package com.github.simplenet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

final class ConnectionTest {

    private static final String HOST = "localhost";

    private static final int PORT = 43594;

    private Client client;

    private Server server;

    private CountDownLatch latch;

    @BeforeEach
    void beforeEach() {
        client = new Client();
        server = new Server();
        latch = new CountDownLatch(1);
        server.bind(HOST, PORT, 1);
    }

    @AfterEach
    void afterEach() throws InterruptedException {
        client.connect(HOST, PORT);

        try {
            if (!latch.await(500L, TimeUnit.MILLISECONDS)) {
                fail();
            }
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void testConnectedClientsAfterServerCloseClient() {
        server.onConnect(client -> {
            assertEquals(1, server.getNumConnectedClients());
            client.close();
            assertEquals(0, server.getNumConnectedClients());
            latch.countDown();
        });
    }
}