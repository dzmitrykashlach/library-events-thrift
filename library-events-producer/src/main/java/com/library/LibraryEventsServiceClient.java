package com.library;

import com.thrift.impl.LibraryEvent;
import com.thrift.impl.LibraryService;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsServiceClient {

    public void save(LibraryEvent libraryEvent) {
        try (TTransport transport = new TSocket("localhost", 9090)) {

            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            LibraryService.Client client = new LibraryService.Client(protocol);

            log.info("Saving event to library =>" + libraryEvent.toString());

            client.save(libraryEvent);

            log.info("Saved to library " + libraryEvent);

        } catch (TException e) {
            log.error("Event was not saved: ", e);
        }
    }
}

