import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thrift.LibraryEventsConsumerApplication;
import com.thrift.consumer.LibraryEventsConsumer;
import com.thrift.entity.Book;
import com.thrift.entity.LibraryEvent;
import com.thrift.entity.LibraryEventType;
import com.thrift.jpa.LibraryEventsRepository;
import com.thrift.service.LibraryEventService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = LibraryEventsConsumerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.IntegerSerializer"
        , "retryListener.startup=false"
})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer consumer;

    private Consumer<Integer, String> topicConsumer;

    @SpyBean
    LibraryEventService service;

    @Autowired
    LibraryEventsRepository repository;

    @Autowired
    ObjectMapper mapper;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;


    @BeforeEach
    public void setUp() {
        var container = endpointRegistry.getListenerContainers()
                .stream().filter(messageListenerContainer -> messageListenerContainer.getGroupId()
                        .equals("library-events-listener-group"))
                .collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        /* for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        }*/
    }

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Zahadki bez adhadki\",\"bookAuthor\":\"Ramanauskaya Slabada\"}}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);
        verify(consumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(service, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> events = (List<LibraryEvent>) repository.findAll();
        assert events.size() == 1;
        events.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assert libraryEvent.getBook().getBookId() == 123;
        });
    }

    @Test
    public void publishUpdateLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Zahadki bez adhadki\",\"bookAuthor\":\"Ramanauskaya Slabada\"}}";

        LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        //publish the update LibraryEvent
        Book updatedBook = Book.builder().
                bookId(123).bookName("Zahadki bez adhadki Ultimate Edition").bookAuthor("Romavaya Baba").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = mapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(consumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(service, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = repository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Zahadki bez adhadki Ultimate Edition", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    public void publishUpdateLibraryEventInvalid() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Zahadki bez adhadki\",\"bookAuthor\":\"Ramanauskaya Slabada\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);

        verify(consumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(service, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs =
                new HashMap<>(KafkaTestUtils.consumerProps(
                        embeddedKafka.getBrokersAsString(), "group2", "true"));
        topicConsumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(topicConsumer, dltTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(topicConsumer, dltTopic);
        assertEquals(json, consumerRecord.value());

    }

    @Test
    public void publishUpdateLibraryEvent_999() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Zahadki bez adhadki\",\"bookAuthor\":\"Ramanauskaya Slabada\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);

        verify(consumer, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(service, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs =
                new HashMap<>(KafkaTestUtils.consumerProps(
                        embeddedKafka.getBrokersAsString(), "group1", "true"));
        topicConsumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(topicConsumer, retryTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(topicConsumer, retryTopic);
        assertEquals(json, consumerRecord.value());
    }
}
