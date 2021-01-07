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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import io.pravega.client.ClientConfig;
import io.pravega.common.concurrent.Futures;
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

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class HelloWorldWriter {

    public final String scope;
    public final String streamName;
    public final URI controllerURI;
    public final int NUM_WRITERS = 3;
    public final int NUM_EVENTS = 20000;

    public HelloWorldWriter(String scope, String streamName, URI controllerURI) {
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
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        List<EventStreamWriter<String>> writers = new ArrayList<>();

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build())) {

            for (int i = 0; i < NUM_WRITERS; i++) {
                writers.add(clientFactory.createEventWriter(streamName,
                        new JavaSerializer<String>(),
                        EventWriterConfig.builder().build()));
            }

            System.out.format("Writing first batch of events.");
            System.out.format("%n");
            for (int i = 0; i < NUM_WRITERS; i++) {
                writerList.add(write(writers.get(i), NUM_EVENTS, routingKey, message));
            }
            Futures.allOf(writerList).get();

            writerList.clear();

            sleep(150000);

            System.out.format("Writing second batch of events.");
            System.out.format("%n");
            for (int i = 0; i < NUM_WRITERS; i++) {
                writerList.add(write(writers.get(i), NUM_EVENTS, routingKey, message));
            }
            Futures.allOf(writerList).get();
        }

    }

    private CompletableFuture<Void> write(EventStreamWriter<String> writer, long num_events, String routingKey, String message) {
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < num_events; i++) {
                // data.incrementAndGet();
                writer.writeEvent(routingKey, message);
                writer.flush();
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
        
        HelloWorldWriter hww = new HelloWorldWriter(scope, streamName, controllerURI);
        
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
