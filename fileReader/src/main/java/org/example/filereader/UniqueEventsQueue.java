package org.example.filereader;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class UniqueEventsQueue<T> {
    private final Queue<T> queue;
    private int size;
    private final int capacity;

    public UniqueEventsQueue(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
        this.capacity = capacity;
    }

    public synchronized void add(T element) throws InterruptedException {
        while(queue.size() == capacity){
            wait();
        }
        queue.add(element);
        size++;
        notifyAll();
    }

    public synchronized T get() throws InterruptedException {
        while (queue.isEmpty()) {
            wait();
        }
        T elementToRemove = queue.poll();
        size--;
        notify();
        return elementToRemove;
    }

    public int size(){
        return size;
    }
}


