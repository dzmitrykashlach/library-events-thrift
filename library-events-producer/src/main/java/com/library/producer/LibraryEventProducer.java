package com.library.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.library.LibraryEventsServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import com.thrift.impl.LibraryEvent;

@Component
@Slf4j
public class LibraryEventProducer {


    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsServiceClient client;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws IOException, TException {
        client.save(libraryEvent);
    }
}
