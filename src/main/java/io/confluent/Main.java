package io.confluent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final String BOOTSTRAP_URLS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String JSON_SERIALIZER = "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer";

    private static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String JSON_DESERIALIZER = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer";

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException, RestClientException {

        // Some helper classes.
        final Random random = new Random();
        final ObjectMapper mapper = new ObjectMapper();
        final SchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 10);

        // Producers will share topic which is not a recommended pattern in general.
        final String topic = "test-topic";
        final String subject = String.format("%s-value", topic);

        // Ensure that the schema we expect is deployed.
        final ParsedSchema parsedSchema = new JsonSchema(SCHEMA);
        final int version = client.register(subject, parsedSchema);
        System.out.println(String.format("Registered subject: %s schema id: %d", subject, version));
        System.out.println(String.format("Registered raw schema text: %s", parsedSchema.rawSchema()));

        // Lookup schema as it is deployed (can differ syntactically from deployed version)
        final SchemaMetadata schemaMetadata = client.getSchemaMetadata(subject, -1);
        final String schemaString = schemaMetadata.getSchema();
        final JsonNode jsonSchema = mapper.readTree(schemaString);
        System.out.println(String.format("Retrieved version: %d", schemaMetadata.getVersion()));
        System.out.println(String.format("Retrieved raw schema text: %s", schemaMetadata.getSchema()));

        // Create a producer that expects the User data as a POJO.
        final Producer<String, User> pojoProducer = createPojoProducer();
        // Create a producer that expects the User data as a JSON.
        final Producer<String, ObjectNode> jsonProducer = createJsonProducer();

        // Write ten messages
        for (int i = 0; i < 10; i++) {
            // Do some randomization
            int number = random.nextInt(4);
            int points = random.nextInt(100);
            String player = String.format("player%d", number);

            // Create POJO producer key and payload
            String pojoKey = String.format("pojo%d", number);
            User pojoValue = new User(player, points);

            // Write POJO record
            ProducerRecord<String, User> pojoRecord = new ProducerRecord<>(topic, pojoKey, pojoValue);
            pojoProducer.send(pojoRecord).get();

            // Serialize the POJO class to it's JSON equivalent (easier than to write and format it).
            JsonNode jsonPayload = mapper.convertValue(pojoValue, JsonNode.class);

            // Create JSON producer key and payload.
            String jsonKey = String.format("json%d", number);
            ObjectNode jsonValue = JsonSchemaUtils.envelope(jsonSchema, jsonPayload);

            // Write JSON record.
            ProducerRecord<String, ObjectNode> jsonRecord = new ProducerRecord<>(topic, jsonKey, jsonValue);
            jsonProducer.send(jsonRecord).get();
        }

        // Create a poison pill.
        final JsonNode poisonPill = mapper.readTree("{\"poison\": \"pill\"}");
        final ObjectNode poisonValue = JsonSchemaUtils.envelope(jsonSchema, poisonPill);
        final ProducerRecord<String, ObjectNode> poisonRecord = new ProducerRecord<>(topic, "", poisonValue);
        try {
            // Attempt to write poison pill.
            jsonProducer.send(poisonRecord).get();
            // This line should not get printed ...
            System.out.println("Poison pill accepted ... WTH?!!");
        }
        catch (SerializationException ex) {
            // We expect this exception to be trapped and written to console.
            System.out.println(String.format("Poison pill rejected! '%s'", ex.getMessage()));
        }

        // Clean up producer resources.
        pojoProducer.close();
        jsonProducer.close();

        // Run POJO consumer.
        final Consumer<String, User> pojoConsumer = createPojoConsumer();
        pojoConsumer.subscribe(Arrays.asList(topic));
        runConsumer(pojoConsumer, "pojoConsumer");

        // Run JSON consumer
        final Consumer<String, JsonNode> jsonConsumer = createJsonConsumer();
        jsonConsumer.subscribe(Arrays.asList(topic));
        runConsumer(jsonConsumer, "jsonConsumer");

        // Verify that outputs are the same.
    }

    private  static  void runConsumer(Consumer<?, ?> consumer, String name) {
        final Duration pollDuration =Duration.ofMillis(100);
        try {
            while (true) {
                ConsumerRecords<?, ?> records = consumer.poll(pollDuration);
                if(records.isEmpty())
                    break;
                for(var record : records) {
                    System.out.println(String.format("%s|key:%s|value:%s", name, record.key(), record.value()));
                }
            }

        } catch (WakeupException we) {
            // Ignore
        } catch (Exception ex) {
            System.out.println(String.format("Json consumer exception '%s'", ex.getMessage()));
        } finally {
            consumer.close();
        }
    }

    private static Producer<String, User> createPojoProducer() {
        Properties props = createProducerProps();
        return new KafkaProducer<>(props);
    }

    private static Producer<String, ObjectNode> createJsonProducer() {
        Properties props = createProducerProps();
        return new KafkaProducer<>(props);
    }

    private static Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URLS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSON_SERIALIZER);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        // In a production environment, this should ideally be blocked server side.
        props.put("auto.register.schemas", false);

        // Beware of performance impact when comparing Raw JSON to schema.
        // This is defaulted to false, then again, then what is the point of a schema not enforced.
        props.put("json.fail.invalid.schema", true);

        return props;
    }

    private  static Consumer<String, User> createPojoConsumer() {
        Properties props = createConsumerProps("pojo-consumer");
        return new KafkaConsumer<>(props);
    }

    private  static Consumer<String, JsonNode> createJsonConsumer() {
        Properties props = createConsumerProps("json-consumer");
        return new KafkaConsumer<>(props);
    }

    private static Properties createConsumerProps(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URLS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JSON_DESERIALIZER);
        props.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return props;
    }

    public static class User {
        @JsonProperty
        public String name;
        @JsonProperty
        public int points;

        public User(String name, int points) {
            this.name = name;
            this.points = points;
        }
    }

    // The schemas are extremely sensitive, a slight change could break compatibility
    // private static final String SCHEMA =
    //        "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"User\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},\"points\":{\"type\":\"integer\"}},\"required\":[\"points\"]}";
    private static final String SCHEMA = """
    {
        "$schema":"http://json-schema.org/draft-07/schema#",
        "title":"User",
        "type":"object",
        "additionalProperties":false,
        "properties": {
            "name": {
                "oneOf": [
                    {
                        "type": "null",
                        "title":"Not included"
                    },
                    {
                        "type":"string"
                    }
                ]
            },
            "points": {
                "type": "integer"
            }
        },
        "required": [
            "points"
        ]
    }
    """;
            
}
