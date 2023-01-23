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
    private final AtomicBoolean isPoisonFound;

    public Consumer(UniqueEventsQueue<String[]> queue, AtomicInteger counter, String order, int sortIndex, boolean isInt, AtomicBoolean isPoisonFound) {
        this.queue = queue;
        this.counter = counter;
        this.order = order;
        this.sortIndex = sortIndex;
        this.isInt = isInt;
        this.isPoisonFound = isPoisonFound;
    }

    @Override
    public void run() {
        String[] elements;
        while (!isPoisonFound.get()) {
            try {
                elements = peekPoll();
                mergesort(elements, order, sortIndex, isInt);
                writeSortedFile(elements, counter.getAndIncrement());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException();
            }
        }
        System.out.println(this.getName() + " has finished its' work!");
    }

    private synchronized String[] peekPoll() throws InterruptedException {
        queue.peek();
        if (queue.peek()[0].equals("POISONPILL")) {
            isPoisonFound.set(true);
        }else {
            return queue.poll();
        }
        return new String[]{"POISONPILL"};
    }
}
