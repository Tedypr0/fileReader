package org.example.filereader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.filereader.Consumer.isPoisonFound;

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
    private long threadPoolSize = 1;
    private final UniqueEventsQueue<String[]> queue = new UniqueEventsQueue<>(32);
    long lines = 0;
    long maxElements = 10;    //Write here how many lines each temp file will have.
    String sortFileDir = "C:\\csv\\100.csv";
    String fileNames = "sortedGeneration";
    String tempFileDir = "C:\\csv\\sortedFiles\\";
    AtomicInteger counter;
    long count = 0;
    public int userSortDecisionIndex;
    private boolean intOrNot;
    private String areInt;
    private String ascOrDesc;
    private final List<Consumer> threads;

    public ExternalSorter() {
        threads = new ArrayList<>();
        counter = new AtomicInteger(0);
    }

    public void start() {
        int slices = 0;
        String line;
        String firstRow = null;
        try (FileReader lineFile = new FileReader(sortFileDir); BufferedReader lineCounter = new BufferedReader(lineFile)) {
            //Reads the first line of the csv file which usually contains (name,age,email..etc).
            firstRow = lineCounter.readLine();
            // Read the second line which contains real data (Teodor,13)
            line = lineCounter.readLine();
            areInt = line;

            // Count the lines of our file.
            // O(number of lines) linear.
            /* Counts the number of lines and checks if there is invalid data. Every line must be valid.
             * If we have name and age as the first line
             * (Teodor,5) is valid and (Teodor) is invalid.
             */
            do {
                if (firstRow.split(",").length != line.split(",").length) {
                    continue;
                }
                lines++;
            } while ((line = lineCounter.readLine()) != null);
            // Calculate into how many slices our file has to be split.
            slices = (int) Math.ceil((double) lines / maxElements);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileReader fileToSort = new FileReader(sortFileDir); BufferedReader buffer = new BufferedReader(fileToSort)) {

            //Saves first line of csv which contains sort options.

            assert firstRow != null;
            String[] sortOptionsArr = buffer.readLine().split(",");
            String[] userSortDecision;
            userSortDecision = getUserInput(firstRow);

            // Set the field index our user wants to sort by.
            userSortDecisionIndex = setValuesToUserSortDecisionIndexes(sortOptionsArr, userSortDecision);

            // User input validation that input data exists as an option and set indexes of which elements are going to be sorted


            intOrNot = false;

            // Find out if the user input sort by is an integer or a string.
            try {
                Integer.parseInt(areInt.split(",")[userSortDecisionIndex]);
                intOrNot = true;
            } catch (NumberFormatException ignored) {
            }
            // Create and start threads.
            threadCalculatorAndStarter();

            /* Variable for the last file number. Example: we have a file with 95 lines and every temp file is being split
                into 20 lines. The last file is going to be 15 lines long.
             */

            long lastFile = maxElements;
            String[] elements = new String[(int) maxElements];
            // O(slices*lastFile) complexity which is linear.
            for (int i = 0; i < slices; i++) {
                for (int j = 0; j < lastFile; j++) {
                    line = buffer.readLine();
                    // Initialize every index of the elements array and replace all chars which do not belong in the ascii.
                    elements[j] = line.replaceAll("\\P{ASCII}", "");
                }
                //Write slices

                try {
                    // sleep(5000);
                    queue.add(elements);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //Checks if the last slice has to be less than maxElements and if it does, creates a new array with leftover elements.
                if (i >= (slices - 2) && (lines % maxElements != 0)) {
                    lastFile = (int) (lines % maxElements);
                    elements = new String[(int) lastFile];
                } else {
                    elements = new String[(int) maxElements];
                }
            }
            // Adds the poison-pill array which stops the consumer from consuming.
            elements = new String[]{"POISONPILL"};
            queue.add(elements);
        } catch (IOException | InterruptedException e) {
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
            try {
                for (BufferedReader reader : readers) {
                    reader.close();
                }
                f.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //Introduce a second var for when comparing strings.
        int min;
        String stringToAdd;
        String[] elements;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("%sresult.csv", tempFileDir)))) {
            assert firstRow != null;
            writer.write(firstRow);
            writer.newLine();

            //Reads all Files
            // O(Number of readers * maxElements) which equals to the number of lines in the csv file.
            while (true) {
                if (ascOrDesc.equalsIgnoreCase("asc")) {
                    min = Integer.MAX_VALUE;
                    stringToAdd = "~";
                } else {
                    min = Integer.MIN_VALUE;
                    stringToAdd = "!";
                }


                // Reads finds min value for each reader
                for (int k = 0; k < slices; k++) {
                    if (firstLines[k] != null) {
                        elements = firstLines[k].split(",");
                        // Checks if the value the user wants to sort by is an int or not.
                        if (intOrNot) {
                            if (ascOrDesc.equalsIgnoreCase("asc")) {
                                if (min >= Integer.parseInt(elements[userSortDecisionIndex])) {
                                    min = Integer.parseInt(elements[userSortDecisionIndex]);
                                }
                            } else {
                                if (min <= Integer.parseInt(elements[userSortDecisionIndex])) {
                                    min = Integer.parseInt(elements[userSortDecisionIndex]);
                                }
                            }
                        } else {
                            if (ascOrDesc.equalsIgnoreCase("asc")) {
                                if (stringToAdd.compareTo(elements[userSortDecisionIndex]) > 0) {
                                    stringToAdd = elements[userSortDecisionIndex];
                                }
                            } else {
                                if (stringToAdd.compareTo(elements[userSortDecisionIndex]) < 0) {
                                    stringToAdd = elements[userSortDecisionIndex];
                                }
                            }
                        }
                    }
                }
                count = 0;
                //Writes every value which is == to min and continues on.
                for (int j = 0; j < slices; j++) {

                    if (firstLines[j] != null) {
                        elements = firstLines[j].split(",");
                        // This will be only for integers. Need something for strings

                        if (intOrNot) {
                            if (min == Integer.parseInt(elements[userSortDecisionIndex])) {
                                writer.write(firstLines[j]);
                                writer.newLine();
                                firstLines[j] = readers.get(j).readLine();
                            }
                        } else {
                            if (stringToAdd.equals(elements[userSortDecisionIndex])) {
                                writer.write(firstLines[j]);
                                writer.newLine();
                                firstLines[j] = readers.get(j).readLine();
                            }
                        }
                    }
                    // The slowest part of the algorithm depending on the input data
                    // If data contains only unique numbers is the worst scenario
                    // O(number of lines * slices).
                }

                //Checks if all firstLines are null, if a firstLine is null it means that its corresponding reader has finished reading and increments count.

                for (int i = 0; i < slices; i++) {
                    if (firstLines[i] != null) {
                        break;
                    } else {
                        count++;
                    }
                }

                //Closes all readers final iteration to check
                if (count == slices) {
                    for (int i = 0; i < slices; i++) {
                        closeAndDeleteFile(readers, i);
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

    }

    public String[] getUserInput(String sortOptions) {
        String[] userSortDecision;
        System.out.println("Write what you want to sort by.");
        System.out.printf("You can sort by %s%n", sortOptions);
        System.out.println("Write in what order you want to sort: asc or desc with a comma");
        try (Scanner scanner = new Scanner(System.in)) {
            userSortDecision = scanner.nextLine().split(",");
            ascOrDesc = userSortDecision[1];
        }
        return userSortDecision;
    }

    // Validate that we actually have the field the user wants to sort by.
    private int setValuesToUserSortDecisionIndexes(String[] sortOptions, String[] userSortDecision) {
        int result = 0;
        for (int i = 0; i < sortOptions.length; i++) {
            if (sortOptions[i].equalsIgnoreCase(userSortDecision[0])) {
                result = i;
            }
        }
        return result;
    }

    public static void mergesort(String[] str, String ascOrDesc, int sortIndex, boolean isInt) {
        int low = 0;
        int high = str.length - 1;
        String[] temp = Arrays.copyOf(str, str.length);
        for (int m = 1; m <= high - low; m = 2 * m) {
            for (int i = low; i < high; i += 2 * m) {
                int mid = i + m - 1;
                int to = Integer.min(i + 2 * m - 1, high);
                merge(str, temp, i, mid, to, sortIndex, isInt);
            }
        }
        if (ascOrDesc.equalsIgnoreCase("desc")) {
            arrayReverse(str);
        }
    }

    private static void merge(String[] strings, String[] temp, int from, int mid, int to, int sortIndex, boolean isInt) {
        // Must add comparison logic for different elements => int and strings.
        int k = from, i = from, j = mid + 1;
        while (i <= mid && j <= to) {

            String[] leftElement = strings[i].split(",");
            String[] rightElement = strings[j].split(",");

            if (isInt) {
                if ((Integer.parseInt(leftElement[sortIndex])) < Integer.parseInt(rightElement[sortIndex])) {
                    temp[k++] = strings[i++];
                } else {
                    temp[k++] = strings[j++];
                }
            } else {
                if (leftElement[sortIndex].compareTo(rightElement[sortIndex]) < 0) {
                    temp[k++] = strings[i++];
                } else {
                    temp[k++] = strings[j++];
                }
            }
        }

        while (i < strings.length && i <= mid) {
            temp[k++] = strings[i++];
        }

        for (i = from; i <= to; i++) {
            strings[i] = temp[i];
        }
    }

    private static void arrayReverse(String[] strings) {
        int first = 0;
        int last = strings.length - 1;
        String tempString;
        while (first < last) {
            tempString = strings[first];
            strings[first] = strings[last];
            strings[last] = tempString;
            first++;
            last--;
        }
    }

    //Writes and saves temp files.
    synchronized static void writeSortedFile(String[] elements, int sliceNumber) throws IOException {
        try (FileWriter sortedFile = new FileWriter(String.format("C:\\csv\\sortedFiles\\sortedGeneration%d.csv", sliceNumber));
             BufferedWriter writer = new BufferedWriter(sortedFile)) {
            for (String a : elements) {
                writer.write(a);
                writer.newLine();
            }
        }
    }

    public void closeAndDeleteFile(List<BufferedReader> readers, int index) {
        try {
            readers.get(index).close();
            Files.delete(Paths.get(String.format("%s%s%d.csv", tempFileDir, fileNames, index)));
        } catch (NoSuchFileException ignored) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Calculates how many threads we need for the current file, adds them to an ArrayList and starts them.
        Min threads: 1
        Маx threads: 8
     */
    private void threadCalculatorAndStarter() {
        if (lines / maxElements == 0) {
            maxElements = lines;
        } else if (lines / maxElements > 7) {
            threadPoolSize = 8;
        } else {
            threadPoolSize = lines / maxElements;
            if (lines % maxElements != 0) {
                threadPoolSize++;
            }
        }
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        for (int i = 0; i < threadPoolSize; i++) {
            threads.add(new Consumer(queue, counter, ascOrDesc, userSortDecisionIndex, intOrNot, atomicBoolean));
            threads.get(i).start();
        }
    }

    public static synchronized String[] peekPoll(UniqueEventsQueue<String[]> queue) throws InterruptedException {
        queue.peek();
        if (queue.peek()[0].equals("POISONPILL")) {
            isPoisonFound.set(true);
        } else {
            return queue.poll();
        }
        return null;
    }
}