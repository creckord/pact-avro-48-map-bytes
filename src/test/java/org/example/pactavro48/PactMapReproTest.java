package org.example.pactavro48;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import au.com.dius.pact.consumer.dsl.PactBuilder;
import au.com.dius.pact.consumer.junit5.PactConsumerTest;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.V4Interaction;
import au.com.dius.pact.core.model.V4Pact;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.model.v4.MessageContents;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@PactConsumerTest
@PactTestFor(providerName = "example-provider", providerType = ProviderType.ASYNCH, pactVersion = PactSpecVersion.V4)
class PactMapReproTest {

    private static final String STRING_MAP_SCHEMA = """
            [{
                "type": "record",
                "name": "ExampleEvent",
                "namespace": "org.example",
                "fields": [
                  {
                    "name": "id",
                    "type": "string"
                  },
                  {
                    "name": "hashes",
                    "type": {
                      "type": "map",
                      "values": "string"
                    }
                  }
                ]
            }]
            """;

    private static final String BYTES_MAP_SCHEMA = """
            [{
                "type": "record",
                "name": "ExampleEvent",
                "namespace": "org.example",
                "fields": [
                  {
                    "name": "id",
                    "type": "string"
                  },
                  {
                    "name": "hashes",
                    "type": {
                      "type": "map",
                      "values": "bytes"
                    }
                  }
                ]
            }]
            """;

    private static final String BYTES_FIELD_SCHEMA = """
            [{
                "type": "record",
                "name": "ExampleEvent",
                "namespace": "org.example",
                "fields": [
                  {
                    "name": "id",
                    "type": {
                      "type": "string"
                    }
                  },
                  {
                    "name": "hash",
                    "type": "bytes"
                  }
                ]
            }]
            """;

    @TempDir
    static Path schemaDir;

    @Pact(consumer = "example-consumer")
    V4Pact matchStringKeys(PactBuilder builder) throws Exception {
        final String schemaContent = STRING_MAP_SCHEMA;
        final Path schemaFile = Files.createTempFile(schemaDir, "schema-", ".avsc");
        Files.writeString(schemaFile, schemaContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return builder.usingPlugin("avro", "0.0.6")
                .expectsToReceive("Example event with string map", "core/interaction/message")
                .with(Map.of("message.contents",
                        ofEntries(
                                entry("pact:avro", schemaFile.toString()),
                                entry("pact:record-name", "ExampleEvent"),
                                entry("pact:content-type", "avro/binary"),
                                entry("id", "notEmpty('12345')"),
                                entry("hashes", Map.of("sha-256",
                                        "matching(regex, '^[a-fA-F0-9]{64}$', 'b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51')"))
                        )))
                .toPact();
    }

    @Pact(consumer = "example-consumer")
    V4Pact matchStringMap(PactBuilder builder) throws Exception {
        final String schemaContent = STRING_MAP_SCHEMA;
        final Path schemaFile = Files.createTempFile(schemaDir, "schema-", ".avsc");
        Files.writeString(schemaFile, schemaContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return builder.usingPlugin("avro", "0.0.6")
                .expectsToReceive("Example event with string map", "core/interaction/message")
                .with(Map.of("message.contents",
                        ofEntries(
                                entry("pact:avro", schemaFile.toString()),
                                entry("pact:record-name", "ExampleEvent"),
                                entry("pact:content-type", "avro/binary"),
                                entry("id", "notEmpty('12345')"),
                                entry("hashes", ofEntries(
                                        //Neither of these works, either with or without additional example entries: (null value for (non-nullable) string at ExampleEvent.hashes["pact:match"])
                                        entry("pact:match", "eachKey(matching(regex, '^(sha-256|coins)$', 'sha-256'))"),
                                        // entry("pact:match", "eachValue(notEmpty('b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51'))"),
                                        //This one does work:
                                        // entry("pact:match", "atMost(2)"),
                                        entry("sha-256", "notEmpty('b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51')")
                                        ))
                        )))
                .toPact();
    }

    @Pact(consumer = "example-consumer")
    V4Pact matchBytesKeys(PactBuilder builder) throws Exception {
        final String schemaContent = BYTES_MAP_SCHEMA;
        final Path schemaFile = Files.createTempFile(schemaDir, "schema-", ".avsc");
        Files.writeString(schemaFile, schemaContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return builder.usingPlugin("avro", "0.0.6")
                .expectsToReceive("Example event with bytes map", "core/interaction/message")
                .with(Map.of("message.contents",
                        ofEntries(
                                entry("pact:avro", schemaFile.toString()),
                                entry("pact:record-name", "ExampleEvent"),
                                entry("pact:content-type", "avro/binary"),
                                entry("id", "notEmpty('12345')"),
                                //None of these work, see below -> matchBytesField
                                entry("hashes", Map.of("sha-256", "notEmpty('b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51')"))
                                // entry("hashes", Map.of("sha-256", "matching(regex, '^[a-fA-F0-9]{64}$', 'b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51')"))
                                // entry("hashes", Map.of("sha-256", "b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51"))
                                // entry("hashes", Map.of("sha-256", Hex.decodeHex("b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51")))
                                // entry("hashes", Map.of("sha-256", ByteBuffer.wrap(Hex.decodeHex("b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51"))))
                                //The only way I could make this pact work at all:
                                // entry("hashes", Map.of())
                        )))
                .toPact();
    }

    @Pact(consumer = "example-consumer")
    V4Pact matchBytesMap(PactBuilder builder) throws Exception {
        final String schemaContent = BYTES_MAP_SCHEMA;
        final Path schemaFile = Files.createTempFile(schemaDir, "schema-", ".avsc");
        Files.writeString(schemaFile, schemaContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return builder.usingPlugin("avro", "0.0.6")
                .expectsToReceive("Example event with bytes map", "core/interaction/message")
                .with(Map.of("message.contents",
                        ofEntries(
                                entry("pact:avro", schemaFile.toString()),
                                entry("pact:record-name", "ExampleEvent"),
                                entry("pact:content-type", "avro/binary"),
                                entry("id", "notEmpty('12345')"),
                                //This doesn't work either: Expected map value for field 'hashes' but got 'StringValue(atMost(2))'
                                entry("hashes", "atMost(2)")
                        )))
                .toPact();
    }

    @Pact(consumer = "example-consumer")
    V4Pact matchBytesField(PactBuilder builder) throws Exception {
        final String schemaContent = BYTES_FIELD_SCHEMA;
        final Path schemaFile = Files.createTempFile(schemaDir, "schema-", ".avsc");
        Files.writeString(schemaFile, schemaContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return builder.usingPlugin("avro", "0.0.6")
                .expectsToReceive("Example event with bytes map", "core/interaction/message")
                .with(Map.of("message.contents",
                        ofEntries(
                                entry("pact:avro", schemaFile.toString()),
                                entry("pact:record-name", "ExampleEvent"),
                                entry("pact:content-type", "avro/binary"),
                                entry("id", "notEmpty('12345')"),
                                //Neither of these works:
                                //These result in a ClassCastException (String -> ByteBuffer)
                                entry("hash", "notEmpty('b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51')")
                                //  entry("hash", "matching(regex, '^[a-fA-F0-9]{64}$', 'b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51')")
                                //Error parsing expression: Was expecting a matching rule definition type
                                //  entry("hash", "b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51")
                                //These result in a NullPointerException (null value for (non-nullable) bytes at ExampleEvent.hash)
                                //  entry("hash", Hex.decodeHex("b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51"))
                                //  entry("hash", ByteBuffer.wrap(Hex.decodeHex("b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51")))
                        )))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "matchStringKeys")
    void consumerRecordWithMatchedStringKeys(V4Interaction.AsynchronousMessage message)
            throws Exception {
        // This is actually the only one where the pact setup completes - let's check that the value in the map is the example, not the matching expr
        MessageContents messageContents = message.getContents();
        final List<GenericRecord> records = arrayByteToAvroRecord(STRING_MAP_SCHEMA,
                messageContents.getContents().getValue());
        assertEquals(1, records.size());
        final GenericRecord event = records.getFirst();
        final Map<?, ?> hashes = (Map<?, ?>) event.get(event.getSchema().getField("hashes").pos());
        assertEquals(new Utf8("b65a3f6f3fa21f4dd935a6dda082631c301e83936bb5e9b5d1edbad0c135eb51"), hashes.get(new Utf8("sha-256")));
    }

    @Test
    @PactTestFor(pactMethod = "matchStringMap")
    void consumerRecordWithMatchedStringMap(V4Interaction.AsynchronousMessage message)
            throws Exception {
        // Just to have something here - we don't actually reach the test here
        MessageContents messageContents = message.getContents();
        assertNotNull(messageContents.getContents().getValue());
    }

    @Test
    @PactTestFor(pactMethod = "matchBytesKeys")
    void consumerRecordWithMatchedBytesKeys(V4Interaction.AsynchronousMessage message)
            throws Exception {
        //Just to have something here - we don't actually reach the test here
        MessageContents messageContents = message.getContents();
        assertNotNull(messageContents.getContents().getValue());
    }

    @Test
    @PactTestFor(pactMethod = "matchBytesMap")
    void consumerRecordWithMatchedBytesMap(V4Interaction.AsynchronousMessage message)
            throws Exception {
        // Just to have something here - we don't actually reach the test here
        MessageContents messageContents = message.getContents();
        assertNotNull(messageContents.getContents().getValue());
    }

    @Test
    @PactTestFor(pactMethod = "matchBytesField")
    void consumerRecordWithMatchedBytesField(V4Interaction.AsynchronousMessage message)
            throws Exception {
        // Just to have something here - we don't actually reach the test here
        MessageContents messageContents = message.getContents();
        assertNotNull(messageContents.getContents().getValue());
    }

    static List<GenericRecord> arrayByteToAvroRecord(String schemaString, byte[] bytes) throws Exception {
        final Schema schema = new Schema.Parser().parse(schemaString).getTypes().getFirst();
        GenericDatumReader<?> datumReader = new GenericDatumReader<>(schema);
        List<GenericRecord> records = new ArrayList<>();

        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            while (!decoder.isEnd())
                records.add((GenericRecord) datumReader.read(null, decoder));
        }

        return records;
    }

}
