package org.example.filereader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.filereader.ExternalSorter.mergesort;
import static org.example.filereader.ExternalSorter.writeSortedFile;

public class Consumer extends Thread {
    private final AtomicInteger counter;
    private final AtomicInteger neededToBeCreated;
    private final UniqueEventsQueue<String[]> queue;
    private final String order;
    private final int sortIndex;
    private final boolean isInt;

    public Consumer(UniqueEventsQueue<String[]> queue, AtomicInteger counter, AtomicInteger neededToBeCreated, String order, int sortIndex, boolean isInt) {
        this.queue = queue;
        this.counter = counter;
        this.neededToBeCreated = neededToBeCreated;
        this.order = order;
        this.sortIndex = sortIndex;
        this.isInt = isInt;
    }

    @Override
    public void run() {
        do
            try {
                String[] elements = queue.poll();
                mergesort(elements, order, sortIndex, isInt);
                writeSortedFile(elements, counter.getAndIncrement());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException();
            } while ((queue.size() > 0) && (counter.get() < neededToBeCreated.get()));
        System.out.println(this.getName() + " has finished its' work!");
    }
}
