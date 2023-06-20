package com.library;

import com.library.service.thrift.LibraryEventsServer;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ExitCodeEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;


@SpringBootApplication
public class LibraryEventsConsumerApplication {

    @Autowired
    private LibraryEventsServer server;

    public static void main(String[] args){
        SpringApplication.run(LibraryEventsConsumerApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runAfterStartup() throws TTransportException {
        server.start();
    }

    @EventListener(ExitCodeEvent.class)
    public void runOnShutdown(){
        server.stop();
    }



}
