package org.example.filereader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.filereader.ExternalSorter.mergesort;
import static org.example.filereader.ExternalSorter.writeSortedFile;

public class Consumer extends Thread {
    private final AtomicInteger counter;
    private final UniqueEventsQueue<String[]> queue;
    private final String order;
    private final int sortIndex;
    private final boolean isInt;

    public Consumer(UniqueEventsQueue<String[]> queue, AtomicInteger counter, String order, int sortIndex, boolean isInt) {
        this.queue = queue;
        this.counter = counter;
        this.order = order;
        this.sortIndex = sortIndex;
        this.isInt = isInt;
    }

    @Override
    public void run() {
        String[] elements;
        while(true) {
            try {
                if (queue.peek()[0].equals("POISONPILL")){
                    break;
                }
                elements = queue.poll();
                mergesort(elements, order, sortIndex, isInt);
                writeSortedFile(elements, counter.get());
                counter.incrementAndGet();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException();
            }
        }
        System.out.println(this.getName() + " has finished its' work!");
    }
}
