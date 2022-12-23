package org.example.filereader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer extends Thread {
    private final AtomicInteger counter;
    private final UniqueEventsQueue<String[]> queue;
    private volatile boolean isWriterFinished = false;
    private int slices =0;

    public Consumer(UniqueEventsQueue<String[]> queue, AtomicInteger counter, int slices) {
        this.queue = queue;
        this.counter = counter;
        this.slices = slices;
    }

    @Override
    public void run() {
        do
            try {
                String[] elements = queue.get();
                ExternalSorter.mergesort(elements);
                ExternalSorter.writeSortedFile(elements, counter.getAndIncrement());
            } catch (IOException | InterruptedException e) {
               throw new RuntimeException();
            }while (!isWriterFinished && queue.size()>0);
        System.out.println(this.getName() + " has finished its' work!");
    }

    public synchronized void close() {
        isWriterFinished = true;
    }
}
