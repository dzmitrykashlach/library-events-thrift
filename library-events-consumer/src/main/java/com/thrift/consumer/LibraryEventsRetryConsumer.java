package com.thrift.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.thrift.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {

    @Autowired
    LibraryEventService libraryEventService;

    @KafkaListener(topics = {"${topics.retry}"},
            autoStartup = "${retryListener.startup:true}",
    groupId = "retry-listener-group"
    )
    @RetryableTopic
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord in Retry Consumer: {}",consumerRecord);
        consumerRecord.headers().forEach(header -> log.info("Key : {}, value : {}",header.key(),header.value()));
        libraryEventService.processLibraryEvent(consumerRecord);

    }
}
