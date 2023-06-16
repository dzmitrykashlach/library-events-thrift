package com.thrift.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thrift.entity.LibraryEvent;
import com.thrift.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent:{}", libraryEvent);
        if (libraryEvent != null && libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Network issue");
//            log.error("Network issue");
        }
        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> {
                validate(libraryEvent);
                save(libraryEvent);
            }
            default -> log.info("Invalid library event type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id cannot be equal to null");
        }
        Optional<LibraryEvent> eventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if (!eventOptional.isPresent()) {
            throw new IllegalArgumentException("Non-existing library event id");
        }
        log.info("Validation is successful for the library event: {}", eventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("libraryEvent is persisted {} ", libraryEvent);
    }
}
