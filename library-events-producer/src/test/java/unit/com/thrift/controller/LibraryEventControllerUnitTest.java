package com.thrift.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thrift.library.controller.LibraryEventsController;
import com.thrift.library.domain.Book;
import com.thrift.library.domain.LibraryEvent;
import com.thrift.library.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureWebMvc
public class LibraryEventControllerUnitTest {
    @Autowired
    MockMvc mockMvc;

    private ObjectMapper mapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer libraryEventProducer;

    @Test
    public void postLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ramanauskaya Slabada")
                .bookName("Zahadki bez adhadki")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String event = mapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventProducerRecord(isA(LibraryEvent.class));

        mockMvc.perform(post("/v1/libraryevent")
                .content(event)
                .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().isCreated());
    }
    @Test
    public void putLibraryEventNullId() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ramanauskaya Slabada")
                .bookName("Zahadki bez adhadki")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String event = mapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventProducerRecord(isA(LibraryEvent.class));

        mockMvc.perform(put("/v1/libraryevent")
                .content(event)
                .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().isBadRequest());
    }

    @Test
    public void putLibraryEventNotNullId() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ramanauskaya Slabada")
                .bookName("Zahadki bez adhadki")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        String event = mapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventProducerRecord(isA(LibraryEvent.class));

        mockMvc.perform(put("/v1/libraryevent")
                .content(event)
                .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().isOk());
    }

}
