package com.thrift.library.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thrift.library.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        CompletableFuture<SendResult<Integer, String>> cf =
                kafkaTemplate.sendDefault(key, value);
        cf.thenAccept(result -> {
                    handleSuccess(key, value, result);
                })
                .handle((result, throwable) -> {
                    handleFailure(throwable);
                    return null;
                });


    }


    public void sendLibraryEventProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        String topic = "library-events";
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
        CompletableFuture<SendResult<Integer, String>> cf =
                kafkaTemplate.send("library-events", key, value);
        cf.thenAccept(result -> {
                    handleSuccess(key, value, result);
                })
                .handle((result, throwable) -> {
                    handleFailure(throwable);
                    return null;
                });


    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        CompletableFuture<SendResult<Integer, String>> cf =
                kafkaTemplate.sendDefault(key, value);
        SendResult<Integer, String> sendResult;
        try {
            sendResult = cf.get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException | InterruptedException while sending the message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception while sending the message and the exception is {}", e.getMessage());
            throw e;
        }
        return sendResult;
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message is sent successfully for the key: {} and the value is {}, partition is {}",
                key, value, result.getRecordMetadata().partition());
    }

    private void handleFailure(Throwable t) {
        log.error("Error sending message with exception {}", t.getMessage());
        try {
            throw t;
        } catch (Throwable e) {
            log.error("Error in exception handler:{}", e.getMessage());
        }
    }

}
