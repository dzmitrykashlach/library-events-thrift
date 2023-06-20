package com.library.service.thrift;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.library.jpa.LibraryEventsRepository;
import com.thrift.impl.LibraryEvent;
import com.thrift.impl.LibraryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class LibraryThriftServiceImpl implements LibraryService.Iface {
    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Override
    public LibraryEvent get(final int id) {
        throw new RuntimeException("Method is not implemented");
    }

    @Override
    public void save(final LibraryEvent libraryEvent) {
        log.info("Received libraryEvent =>" + libraryEvent.toString());
        com.library.entity.LibraryEvent le = createLibraryEvent(libraryEvent);
        try {
            processLibraryEvent(le);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<LibraryEvent> getList() {
        throw new RuntimeException("Method is not implemented");
    }

    @Override
    public boolean ping() {
        throw new RuntimeException("Method is not implemented");
    }

    private com.library.entity.LibraryEvent createLibraryEvent(LibraryEvent libraryEvent) {
        com.library.entity.Book b = mapper.convertValue(libraryEvent.book, com.library.entity.Book.class);
        com.library.entity.LibraryEvent le =
                new com.library.entity.LibraryEvent(libraryEvent.libraryEventId,
                        libraryEvent.type, b);
        return le;
    }

    public void processLibraryEvent(com.library.entity.LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("libraryEvent:{}", libraryEvent);
        if (libraryEvent != null && libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Network issue");
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

    private void validate(com.library.entity.LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id cannot be equal to null");
        }
        Optional<com.library.entity.LibraryEvent> eventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if (!eventOptional.isPresent()) {
            throw new IllegalArgumentException("Non-existing library event id");
        }
        log.info("Validation is successful for the library event: {}", eventOptional.get());
    }

    private void save(com.library.entity.LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("libraryEvent is persisted {} ", libraryEvent);
    }
}

