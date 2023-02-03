package com.nio;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public final class TcpDataFlowExample {

    private static final String HOSTNAME = "0.0.0.0";
    private static final int FOURS_PORT = 4444;
    private static final int FIVES_PORT = 5555;
    private static final List<Integer> PORTS = Arrays.asList(FIVES_PORT, FOURS_PORT);
    private static final int BUFFER_CAPACITY = 65535;
    private static final String STOP_READ = "stop-read";
    private static final String START_READ = "start-read";
    private static final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);
    private static final Multimap<Integer, SocketChannel> clients = ArrayListMultimap.create();

    private TcpDataFlowExample() {
    }

    public static void main(final String... args) throws Exception {
        System.out.printf("Tcp Data Flow Example started at %s:%s%n", HOSTNAME, PORTS);
        final Selector selector = Selector.open();

        registerPorts(selector);

        while (!Thread.currentThread().isInterrupted()) {
            System.out.printf("Wait new events..%n");
            selector.select();

            final Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            iterator.forEachRemaining(selectionKey -> processKeys(selector, iterator, selectionKey));
        }
    }

    private static void processKeys(Selector selector, Iterator<SelectionKey> iterator, SelectionKey selectionKey) {
        try {
            acceptKey(selector, selectionKey);

            if (selectionKey.isReadable()) {
                System.out.println("Handle READ event");
                final SocketChannel client = (SocketChannel) selectionKey.channel();
                final int read = client.read(buffer);

                if (read == -1) {
                    client.close();
                    client.keyFor(selector).cancel();
                    System.out.printf("The connection was closed: %s%n", client);
                    return;
                }

                buffer.flip();
                processLocalPort(client.socket().getLocalPort(), client, selector);
                buffer.clear();
            }
            iterator.remove();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void acceptKey(Selector selector, SelectionKey selectionKey) throws IOException {
        if (selectionKey.isAcceptable()) {
            System.out.println("Handle READ event");
            final ServerSocketChannel server = ((ServerSocketChannel) selectionKey.channel());
            final SocketChannel client = server.accept();
            client.configureBlocking(false);
            System.out.printf("New connection accepted: %s%n", client);

            clients.put(server.socket().getLocalPort(), client);

            client.register(selector, SelectionKey.OP_READ);
        }
    }

    private static void registerPorts(Selector selector) throws IOException {
        for (final int port : PORTS) {
            final ServerSocketChannel serverSocket = ServerSocketChannel.open();
            serverSocket.bind(new InetSocketAddress(HOSTNAME, port));
            serverSocket.configureBlocking(false);
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        }
    }

    private static void processLocalPort(int localPort, SocketChannel client, Selector selector) {
        try {
            if (localPort == FOURS_PORT) {
                processFoursPort(selector, client);
            }
            if (localPort == FIVES_PORT) {
                processFivesPort();
                client.write(buffer);
            }
        } catch (IOException e) {
            System.out.printf("Handle exception during write on port: %s", FIVES_PORT);
            e.printStackTrace();
        }
    }

    private static void processFoursPort(Selector selector, SocketChannel client) {
        try {
            final String command = StandardCharsets.UTF_8.decode(buffer).toString();
            if (STOP_READ.equalsIgnoreCase(command.trim())) {
                System.out.println("Handle stop-read");
                registerEvent(selector, 0);
                return;
            }
            if (START_READ.equalsIgnoreCase(command.trim())) {
                System.out.println("Handle start-read");
                registerEvent(selector, SelectionKey.OP_READ);
            } else {
                final byte[] unknownCommand = String.format("Supported commands:%n%s%n%s%n", STOP_READ, START_READ)
                        .getBytes(StandardCharsets.UTF_8);
                client.write(ByteBuffer.wrap(unknownCommand));
            }
        } catch (IOException e) {
            System.out.printf("Handle exception during write on port: %s", FOURS_PORT);
            e.printStackTrace();
        }
    }

    private static void processFivesPort() {
        for (int i = 0; i < buffer.limit(); i++) {
            buffer.put(i, (byte) Character.toUpperCase(buffer.get(i)));
        }
    }

    private static void registerEvent(final Selector selector, final int op) {
        clients.get(FIVES_PORT).forEach(c -> {
            try {
                c.register(selector, op);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
