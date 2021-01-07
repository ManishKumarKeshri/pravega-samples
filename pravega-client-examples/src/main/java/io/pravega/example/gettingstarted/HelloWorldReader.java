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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.Futures;
import lombok.Cleanup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A simple example app that uses a Pravega Reader to read from a given scope and stream.
 */
public class HelloWorldReader {
    private static final int READER_TIMEOUT_MS = 2000;

    public final String scope;
    public final String streamName;
    public final String streamName1;
    public final String streamName2;
    public final String streamName3;
    public final URI controllerURI;

    public HelloWorldReader(String scope, String streamName, String streamName1, String streamName2, String streamName3, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.streamName1 = streamName1;
        this.streamName2 = streamName2;
        this.streamName3 = streamName3;
        this.controllerURI = controllerURI;
    }

    public void run() throws ExecutionException, InterruptedException {
        StreamManager streamManager = StreamManager.create(controllerURI);
        
        final boolean scopeIsNew = streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);
        streamManager.createStream(scope, streamName1, streamConfig);
        streamManager.createStream(scope, streamName2, streamConfig);
        streamManager.createStream(scope, streamName3, streamConfig);

        final String readerGroup1 = UUID.randomUUID().toString().replace("-", "");
        final String readerGroup2 = UUID.randomUUID().toString().replace("-", "");
        final String readerGroup3 = UUID.randomUUID().toString().replace("-", "");

        final ReaderGroupConfig readerGroupConfig1 = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName1))
                .build();
        final ReaderGroupConfig readerGroupConfig2 = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName2))
                .build();
        final ReaderGroupConfig readerGroupConfig3 = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName3))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup1, readerGroupConfig1);
            readerGroupManager.createReaderGroup(readerGroup2, readerGroupConfig2);
            readerGroupManager.createReaderGroup(readerGroup3, readerGroupConfig3);
        }

        int numEventsRead = 0;
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());) {
            System.out.format("Reading all the events from %s/%s%n", scope, streamName);
            System.out.format("Reading all the events from %s/%s%n", scope, streamName1);
            System.out.format("Reading all the events from %s/%s%n", scope, streamName2);
            System.out.format("Reading all the events from %s/%s%n", scope, streamName3);
            List<CompletableFuture<Void>> readersList = new ArrayList<>();
            AtomicLong eventReadCount = new AtomicLong(0);
            readersList.add(startNewReader("reader1", clientFactory, readerGroup1, eventReadCount));
            readersList.add(startNewReader("reader2", clientFactory, readerGroup2, eventReadCount));
            readersList.add(startNewReader("reader3", clientFactory, readerGroup3, eventReadCount));
            Futures.allOf(readersList).get();
            System.out.format("Total number of events read = " + eventReadCount.get());
            System.out.format("%n");
        }
    }

    private CompletableFuture<Void> startNewReader(final String id, final EventStreamClientFactory clientFactory, final String
            readerGroupName, final AtomicLong readCount) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<String> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<String>(),
                    ReaderConfig.builder().build());
            EventRead<String> event = null;
            do {
                event = reader.readNextEvent(SECONDS.toMillis(100));
                if (event.getEvent() != null) {
                    //update if event read is not null.
                    readCount.incrementAndGet();
                }
            } while (event.getEvent() != null);
            reader.close();
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
            formatter.printHelp("HelloWorldReader", options);
            System.exit(1);
        }
        
        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String streamName1 = cmd.getOptionValue("name1") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name1");
        final String streamName2 = cmd.getOptionValue("name2") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name2");
        final String streamName3 = cmd.getOptionValue("name3") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name3");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);
        
        HelloWorldReader hwr = new HelloWorldReader(scope, streamName, streamName1, streamName2, streamName3, controllerURI);
        hwr.run();
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "name", true, "The name of the stream to read from.");
        options.addOption("n1", "name1", true, "The name of the stream to read from.");
        options.addOption("n2", "name2", true, "The name of the stream to read from.");
        options.addOption("n3", "name3", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
