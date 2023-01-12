package org.example.filereader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

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
        do
            try {
                String[] elements = queue.poll();
                ExternalSorter.mergesort(elements, order, sortIndex, isInt);
                ExternalSorter.writeSortedFile(elements, counter.getAndIncrement());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException();
            } while (queue.size() > 0);
        System.out.println(this.getName() + " has finished its' work!");
    }
}
