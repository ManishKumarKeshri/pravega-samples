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
    public final URI controllerURI;

    public HelloWorldReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run() throws ExecutionException, InterruptedException {
        StreamManager streamManager = StreamManager.create(controllerURI);
        
        final boolean scopeIsNew = streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");

        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        int numEventsRead = 0;
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());) {
            System.out.format("Reading all the events from %s/%s%n", scope, streamName);
            List<CompletableFuture<Void>> readersList = new ArrayList<>();
            AtomicLong eventReadCount = new AtomicLong(0);
            readersList.add(startNewReader("reader1", clientFactory, readerGroup, eventReadCount));
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
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);
        
        HelloWorldReader hwr = new HelloWorldReader(scope, streamName, controllerURI);
        hwr.run();
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "name", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
