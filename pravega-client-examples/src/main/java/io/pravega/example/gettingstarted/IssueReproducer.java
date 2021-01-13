/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 */
package io.pravega.example.gettingstarted;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import lombok.Cleanup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class IssueReproducer {

    public final String scope;
    public final String streamName;
    public final URI controllerURI;
    public final int NUM_WRITERS = 3;
    public final int NUM_EVENTS = 10000;
    public final int NUM_TRIES = 5;
    public String lastEvent = null;

    public IssueReproducer(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run(String routingKey, String message) throws ExecutionException, InterruptedException {
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);

        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        List<EventStreamWriter<String>> writers = new ArrayList<>();

        int eventsWritten = 0;
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build())) {

            for (int i = 0; i < NUM_WRITERS; i++) {
                writers.add(clientFactory.createEventWriter(streamName,
                        new JavaSerializer<String>(),
                        EventWriterConfig.builder().build()));
            }

            for (int j = 0; j < NUM_TRIES; j++) {
                System.out.format("Writing first batch of events.");
                System.out.format("%n");
                for (int i = 0; i < writers.size(); i++) {
                    writerList.add(write(writers.get(i), NUM_EVENTS, routingKey));
                }
                Futures.allOf(writerList).get();

                writerList.clear();

                sleep(300000);

                System.out.format("Writing second batch of events.");
                System.out.format("%n");
                for (int i = 0; i < writers.size(); i++) {
                    writerList.add(write(writers.get(i), NUM_EVENTS, routingKey));
                }
                Futures.allOf(writerList).get();

                writerList.clear();

                eventsWritten += 2 * NUM_EVENTS * NUM_WRITERS;

                System.out.format("Total number of events written = " + eventsWritten);
                System.out.format("%n");

                List<CompletableFuture<Void>> readersList = new ArrayList<>();
                AtomicLong eventReadCount = new AtomicLong(0);
                final String readerGroup = UUID.randomUUID().toString().replace("-", "");

                final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                        .stream(Stream.of(scope, streamName))
                        .build();

                try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                    readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
                }

                String readerName = "reader" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
                readersList.add(startNewReader(readerName + j, clientFactory, readerGroup, eventReadCount, eventsWritten));
                Futures.allOf(readersList).get();
                System.out.format("Total number of events read = " + eventReadCount.get());
                System.out.format("%n");

                sleep(300000);
            }
        }

    }

    private CompletableFuture<Void> startNewReader(final String id, final EventStreamClientFactory clientFactory, final String
            readerGroupName, final AtomicLong readCount, int writeCount) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<String> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<String>(),
                    ReaderConfig.builder().build());
            while (readCount.get() < writeCount) {
                String eventString = reader.readNextEvent(SECONDS.toMillis(100)).getEvent();
                if (eventString != null) {
                    //update if event read is not null.
                    readCount.incrementAndGet();
                    if (eventString.equals(lastEvent)) {
                        break;
                    }

                    if (readCount.get() % 10000 == 0) {
                        System.out.println("Read event count: " + readCount.get());
                    }
                }
            }
            reader.close();
        });
    }

    private CompletableFuture<Void> write(EventStreamWriter<String> writer, long num_events, String routingKey) {
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < num_events; i++) {
                // data.incrementAndGet();
                UUID uuid1 = UUID.randomUUID();
                UUID uuid2 = UUID.randomUUID();
                UUID uuid3 = UUID.randomUUID();
                String randomString = uuid1.toString() + uuid2.toString() + uuid3.toString();

                writer.writeEvent(routingKey, randomString);
                writer.flush();

                lastEvent = randomString;
            }
        });
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HelloWorldWriter", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);

        IssueReproducer hww = new IssueReproducer(scope, streamName, controllerURI);
        
        final String routingKey = cmd.getOptionValue("routingKey") == null ? Constants.DEFAULT_ROUTING_KEY : cmd.getOptionValue("routingKey");
        final String message = cmd.getOptionValue("message") == null ? Constants.DEFAULT_MESSAGE : cmd.getOptionValue("message");
        hww.run(routingKey, message);
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "name", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        options.addOption("r", "routingKey", true, "The routing key of the message to write.");
        options.addOption("m", "message", true, "The message to write.");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
