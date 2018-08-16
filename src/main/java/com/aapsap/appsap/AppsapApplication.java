package com.aapsap.appsap;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AppsapApplication {

	public static void main(String[] args) {
		
		SpringApplication.run(AppsapApplication.class, args);

		Properties propiedades = new Properties();
		propiedades.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:3032");
		propiedades.setProperty("key.deserializer", StringDeserializer.class.getName());
		propiedades.setProperty("value.deserializer", StringDeserializer.class.getName());
		propiedades.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appsap");
		propiedades.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		propiedades.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		propiedades.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, Object> topicos = builder.stream("sap");
		topicos.to("bi");
		topicos.to("compra");
		
		KafkaStreams streams = new  KafkaStreams(builder.build(), propiedades);
		streams.start();
	}
}
