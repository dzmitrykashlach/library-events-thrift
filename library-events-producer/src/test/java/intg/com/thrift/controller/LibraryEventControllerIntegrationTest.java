package com.thrift.controller;

import com.thrift.library.domain.Book;
import com.thrift.library.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"},partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    public void setUp() {
        Map<String, Object> configs =
                new HashMap<>(KafkaTestUtils.consumerProps(
                        embeddedKafkaBroker.getBrokersAsString(), "group1", "true"));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    public void tearDown(){
        consumer.close();
    }

    @Test
    public void postLibraryEvent(){

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ramanauskaya Slabada")
                .bookName("Zahadki bez adhadki")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> body = new HttpEntity<>(libraryEvent, httpHeaders);
        ResponseEntity<LibraryEvent> responseEntity =
                testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, body, LibraryEvent.class);
        assertEquals(HttpStatus.CREATED,responseEntity.getStatusCode());
        ConsumerRecord<Integer,String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,"library-events");
        String expectedValue = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Zahadki bez adhadki\",\"bookAuthor\":\"Ramanauskaya Slabada\"}}";
        String actualValue = consumerRecord.value();
        assertEquals(expectedValue,actualValue);
    }

    @Test
    public void putLibraryEventNonNullId(){

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ramanauskaya Slabada")
                .bookName("Zahadki bez adhadki")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> body = new HttpEntity<>(libraryEvent, httpHeaders);
        ResponseEntity<LibraryEvent> responseEntity =
                testRestTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, body, LibraryEvent.class);
        assertEquals(HttpStatus.OK,responseEntity.getStatusCode());
        ConsumerRecord<Integer,String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,"library-events");
        String expectedValue = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Zahadki bez adhadki\",\"bookAuthor\":\"Ramanauskaya Slabada\"}}";
        String actualValue = consumerRecord.value();
        assertEquals(expectedValue,actualValue);
    }

    @Test
    public void putLibraryEventNullId(){
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ramanauskaya Slabada")
                .bookName("Zahadki bez adhadki")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> body = new HttpEntity<>(libraryEvent, httpHeaders);
        ResponseEntity<LibraryEvent> responseEntity =
                testRestTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, body, LibraryEvent.class);
        assertEquals(HttpStatus.BAD_REQUEST,responseEntity.getStatusCode());
        assertEquals("LibraryEvent(libraryEventId=null, libraryEventType=null, book=null)", Objects.requireNonNull(responseEntity.getBody()).toString());
    }
}
