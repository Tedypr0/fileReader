package org.example.filereader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/*
        Sorts a big csv file containing numbers on the second column and writes them in a new file.
        If there isn't a number in the second column the algorithm skips this line.

    TODO: Remove Readers which have already finished executing.
          Remove the corresponding firstLines when a reader has finished its work.  firstLines saves every file's first line.
          Break the while loop when bufferedReaders array reaches 0
          Decrease slices when a reader finishes.
        DONE:  Introduce threads to sorting and writing temp files.
 */

public class ExternalSorter {
    private static final int threadPoolSize = 8;
    private static final UniqueEventsQueue<String[]> queue = new UniqueEventsQueue<>(40);
    static int lines = 0;
    static int maxElements = 200000;    //Write here how many lines each temp file will have.
    static String sortFileDir = "C:\\csv\\10m.csv";
    static String fileNames = "sortedGeneration";
    static String tempFileDir = "C:\\csv\\sortedFiles\\";
    static AtomicInteger counter = new AtomicInteger(0);
    static int count = 0;

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        int slices = 0;
        String line;
        List<Consumer> threads = new ArrayList<>();
        try (FileReader lineFile = new FileReader(sortFileDir);
             BufferedReader lineCounter = new BufferedReader(lineFile)) {

            // Count the lines of our file.
            // O(number of lines) linear.
            while ((line = lineCounter.readLine()) != null) {
                if (line.split(",").length == 1) {
                    continue;
                }
                lines++;
            }
            slices = (int) Math.ceil((double) lines / maxElements);

            for (int i = 0; i < threadPoolSize; i++) {
                threads.add(new Consumer(queue, counter));
                threads.get(i).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (FileReader fileToSort = new FileReader(sortFileDir); BufferedReader buffer = new BufferedReader(fileToSort)) {
            String[] elements = new String[maxElements];

            int lastFile = maxElements;

            // O(slices*lastFile) complexity which is linear.
            for (int i = 0; i < slices; i++) {
                for (int j = 0; j < lastFile; j++) {
                    line = buffer.readLine();
                    elements[j] = line;
                }

                //Write slices
                // sliceNumber++;
                try {
                    queue.add(elements);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //Checks if the last slice has to be less than maxElements and if it does, creates a new array with leftover elements.
                if (i >= (slices - 2) && (lines % maxElements != 0)) {
                    lastFile = lines % maxElements;
                    elements = new String[lastFile];

                } else {
                    elements = new String[maxElements];
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<BufferedReader> readers = new ArrayList<>(slices);
        String[] firstLines = new String[slices];
        try {
            for (Consumer thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Creates and adds into an ArrayList readers for each temp file.
        try {
            for (int i = 0; i < slices; i++) {
                readers.add(new BufferedReader(new FileReader(String.format("%s%s%d.csv", tempFileDir, fileNames, i))));
                line = readers.get(i).readLine();
                firstLines[i] = line;
            }
        } catch (IOException f) {
            f.printStackTrace();
            try {
                for (BufferedReader reader : readers) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int min;
        String[] elements;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("%sresult.csv", tempFileDir)))) {

            //Reads all Files
            // O(Number of readers * maxElements) which equals to the number of lines in the csv file.
            while (true) {
                min = Integer.MAX_VALUE;
                //Reads finds min value for each reader
                for (int k = 0; k < slices; k++) {
                    if (firstLines[k] != null) {
                        elements = firstLines[k].split(",");
                        if (min >= Integer.parseInt(elements[1])) {
                            min = Integer.parseInt(elements[1]);
                        }
                    }
                }
                count = 0;
                //Writes every value which is == to min and continues on.
                for (int j = 0; j < slices; j++) {
                    if (firstLines[j] != null) {
                        elements = firstLines[j].split(",");
                        if (min == Integer.parseInt(elements[1])) {
                            writer.write(firstLines[j]);
                            writer.newLine();
                            firstLines[j] = readers.get(j).readLine();
                        }
                    }
                }

                //Checks if all firstLines are null, if a firstLine is null it means that its corresponding reader has finished reading and increments count.

                for (int i = 0; i < slices; i++) {
                    if (firstLines[i] != null) {
                        break;
                    } else {
                        count++;
                    }
                }

                //Closes all readers.
                if (count == slices) {
                    for (int i = 0; i < slices; i++) {
                        readers.get(i).close();
                        Files.delete(Paths.get(String.format("%s%s%d.csv", tempFileDir, fileNames, i)));
                    }
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            for (BufferedReader reader : readers) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.printf("TIME: %d", (System.currentTimeMillis() - begin));
    }

    public static void merge(String[] strings, String[] temp, int from, int mid, int to) {
        int k = from, i = from, j = mid + 1;
        while (i <= mid && j <= to) {
            String[] leftElement = strings[i].split(",");
            String[] rightElement = strings[j].split(",");
            if ((Integer.parseInt(leftElement[1])) < Integer.parseInt(rightElement[1])) {
                temp[k++] = strings[i++];
            } else {
                temp[k++] = strings[j++];
            }
        }
        while (i < strings.length && i <= mid) {
            temp[k++] = strings[i++];
        }

        for (i = from; i <= to; i++) {
            strings[i] = temp[i];
        }
    }

    public static void mergesort(String[] str) {
        int low = 0;
        int high = str.length - 1;

        String[] temp = Arrays.copyOf(str, str.length);
        for (int m = 1; m <= high - low; m = 2 * m) {
            for (int i = low; i < high; i += 2 * m) {
                int mid = i + m - 1;
                int to = Integer.min(i + 2 * m - 1, high);
                merge(str, temp, i, mid, to);
            }
        }
    }


    public synchronized static void writeSortedFile(String[] elements, int sliceNumber) throws IOException {
        try (FileWriter sortedFile = new FileWriter(String.format("C:\\csv\\sortedFiles\\sortedGeneration%d.csv", sliceNumber)); BufferedWriter writer = new BufferedWriter(sortedFile)) {
            for (String a : elements) {
                writer.write(a);
                writer.newLine();
            }
        }
    }
}