package com.library.service.thrift;

import com.thrift.impl.LibraryService;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsServer {
    private TServer server;
    @Autowired
    private LibraryThriftServiceImpl libraryThriftService;
    public void start() throws TTransportException {
        TServerTransport serverTransport = new TServerSocket(9090);
        server = new TSimpleServer(new TServer.Args(serverTransport)
                .processor(new LibraryService.Processor<>(libraryThriftService)));

        System.out.print("Starting the server... ");

        server.serve();

        System.out.println("done.");
    }

    public void stop() {
        if (server != null && server.isServing()) {
            System.out.print("Stopping the server... ");

            server.stop();

            System.out.println("done.");
        }
    }
}
