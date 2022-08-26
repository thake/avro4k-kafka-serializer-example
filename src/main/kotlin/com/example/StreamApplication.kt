package com.example

import com.github.thake.kafka.avro4k.serializer.Avro4kSerde
import com.github.thake.kafka.avro4k.serializer.KafkaAvro4kDeserializer
import com.github.thake.kafka.avro4k.serializer.KafkaAvro4kDeserializerConfig
import com.github.thake.kafka.avro4k.serializer.KafkaAvro4kSerializer
import java.time.Instant
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import kotlinx.serialization.Serializable
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.measureTimedValue
import kotlin.time.toJavaDuration


const val inputTopic = "input"
const val outputTopic = "output"
const val confluentVersion = "7.2.1"

val logger = LoggerFactory.getLogger("main")
@Serializable
data class Article(
    val title: String,
    val content: String
)
fun main(){
    val confluentCluster = ConfluentCluster(confluentVersion)
    val streamsConfiguration: Properties by lazy {
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = "specific-avro-integration-test"
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] =
            confluentCluster.bootstrapServers
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Avro4kSerde::class.java
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Avro4kSerde::class.java
        streamsConfiguration[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] =
            confluentCluster.schemaRegistryUrl
        streamsConfiguration[KafkaAvro4kDeserializerConfig.RECORD_PACKAGES] =
            Article::class.java.packageName
        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        streamsConfiguration
    }
    val producerConfig: Properties by lazy {
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = confluentCluster.bootstrapServers
        properties[ProducerConfig.ACKS_CONFIG] = "all"
        properties[ProducerConfig.RETRIES_CONFIG] = 0
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = KafkaAvro4kSerializer::class.java
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvro4kSerializer::class.java
        properties[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = confluentCluster.schemaRegistryUrl
        properties
    }
    val consumerConfig: Properties by lazy {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = confluentCluster.bootstrapServers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = "kafka-streams-integration-test-standard-consumer"
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = KafkaAvro4kDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvro4kDeserializer::class.java
        properties[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = confluentCluster.schemaRegistryUrl
        properties[KafkaAvro4kDeserializerConfig.RECORD_PACKAGES] = Article::class.java.packageName
        properties
    }
    val admin = Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to confluentCluster.bootstrapServers))
    //Wait for topic creations
    admin.createTopic(inputTopic)
    admin.createTopic(outputTopic)

    //Input values
    val staticInput = listOf(
        Article("Kafka Streams and Avro4k", "Just use avro4k-kafka-serializer"),
        Article("Lorem ipsum", "another content")
    )
    //Now start kafka streams
    val streamsBuilder = StreamsBuilder()
    streamsBuilder.stream<String, Article>(inputTopic).to(outputTopic)
    val streams = KafkaStreams(streamsBuilder.build(), streamsConfiguration)
    logger.info("Starting Kafka Streams application.")
    streams.start()
    logger.info("Kafka Streams started.")
    
    logger.info("Producing input...")
    produceArticles(staticInput, producerConfig)

    logger.info("Retrieving output from kafka streams pipeline.")
    val values = readValues(consumerConfig)

    logger.info("Input: $staticInput, Output: $values")
    

    //Close the stream after the test
    streams.close()

    confluentCluster.stop()
}

private fun Admin.createTopic(name: String) {
    createTopics(listOf(NewTopic(name, 1, 1))).all().get()
}

private fun produceArticles(articles: Collection<Article>, producerConfig: Properties) {
    val producer: Producer<String, Article> = KafkaProducer(producerConfig)
    articles.forEach { article ->
        producer.send(ProducerRecord(inputTopic, null, Instant.now().toEpochMilli(), article.title, article)).get()
    }
    producer.flush()
    producer.close()
}

@OptIn(ExperimentalTime::class)
private fun readValues(consumerConfig: Properties): List<KeyValue<String, Article>> {
    val consumer: KafkaConsumer<String, Article> = KafkaConsumer(consumerConfig)
    consumer.subscribe(listOf(outputTopic))
    val pollInterval = 100.milliseconds.toJavaDuration()
    val maxTotalPollTime = 10000.milliseconds
    var totalPollTimeMs: Duration = 0.milliseconds
    val consumedValues: MutableList<KeyValue<String, Article>> = mutableListOf()

    while (totalPollTimeMs < maxTotalPollTime) {
        val timedValue = measureTimedValue { consumer.poll(pollInterval) }
        totalPollTimeMs += timedValue.duration
        for (record in timedValue.value) {
            consumedValues.add(KeyValue(record.key(), record.value()))
        }
    }
    consumer.close()
    return consumedValues
}