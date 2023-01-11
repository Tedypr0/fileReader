package org.example.filereader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
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
    private static final int threadPoolSize = 1;
    private static final UniqueEventsQueue<String[]> queue = new UniqueEventsQueue<>(32);
    static long lines = 1;
    static int maxElements = 5;    //Write here how many lines each temp file will have.
    static String sortFileDir = "C:\\csv\\10rev.csv";
    static String fileNames = "sortedGeneration";
    static String tempFileDir = "C:\\csv\\sortedFiles\\";
    static AtomicInteger counter = new AtomicInteger(0);
    static long count = 0;
    private static int[] userSortDecisionIndexes;
    private static boolean[] intOrNot;
    private static String[] areInt;
    private static String ascOrDesc;

    public static void main(String[] args) {
        long begin = System.nanoTime();
        int slices = 0;
        String line;
        String firstRow = null;
        List<Consumer> threads = new ArrayList<>();
        try (FileReader lineFile = new FileReader(sortFileDir); BufferedReader lineCounter = new BufferedReader(lineFile)) {
            firstRow = lineCounter.readLine();

            areInt = lineCounter.readLine().split(",");


            // Count the lines of our file.
            // O(number of lines) linear.
            try {
                while ((line = lineCounter.readLine()) != null) {
                    if (line.split(",").length == 1) {
                        continue;
                    }
                    lines++;
                }
            } catch (OutOfMemoryError e) {
                System.out.println(lines);
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
            //Saves first line of csv which contains sort options.

            String[] sortOptions = buffer.readLine().split(",");
            int lastFile = maxElements;

            /*
                sortOptions gets the first line and it should be printed
                Wait for the user to select by which column/s he wants to sort by
                If nothing is given as an input or it's wrong it will sort by first columns
             */
            System.out.println("Write what you want to sort by separating each word with a comma");
            System.out.printf("You can sort by %s%n", Arrays.toString(sortOptions));
            String[] userSortDecision;
            try (Scanner scanner = new Scanner(System.in)) {
                userSortDecision = scanner.nextLine().split(",");
                System.out.println("Write in what order you want to sort: asc or desc");
                ascOrDesc = scanner.nextLine();
            }

            userSortDecisionIndexes = new int[userSortDecision.length];

            // User input validation that input data exists as an option and set indexes of which elements are going to be sorted

            int counter = 0;
            for (String s : userSortDecision) {
                for (int i = 0; i < sortOptions.length; i++) {
                    if (sortOptions[i].contains(s)) {
                        userSortDecisionIndexes[counter] = i;
                        counter++;
                        break;
                    }
                }
                if (counter == userSortDecision.length) {
                    break;
                }
            }
            if (counter != userSortDecision.length) {
                System.exit(-1);
            }

            Arrays.sort(userSortDecisionIndexes);
            intOrNot = new boolean[userSortDecisionIndexes.length];
            // Find out which column is an integer or a string
            for (int num : userSortDecisionIndexes) {
                try {
                    Integer.parseInt(areInt[num]);
                    if(userSortDecisionIndexes.length == 1){
                        intOrNot[0] = true;
                    }else {
                        intOrNot[num] = true;
                    }
                } catch (NumberFormatException ignored) {
                }
            }

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
                    lastFile = (int) (lines % maxElements);
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
            try {
                for (BufferedReader reader : readers) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //Introduce a second var for when comparing strings.
        int min;
        int stringComparisonMin;
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
                    stringComparisonMin = Integer.MAX_VALUE;
                    stringToAdd = "~";
                } else {
                    min = Integer.MIN_VALUE;
                    stringComparisonMin = Integer.MIN_VALUE;
                    stringToAdd = "!";
                }

                // Reads finds min value for each reader
                //
                for (int k = 0; k < slices; k++) {
                    if (firstLines[k] != null) {
                        elements = firstLines[k].split(",");
                        for (int i = 0; i < intOrNot.length; i++) {
                            int index = userSortDecisionIndexes[i];
                            if (intOrNot[i]) {
                                if (ascOrDesc.equalsIgnoreCase("asc")) {
                                    if (min >= Integer.parseInt(elements[index])) {
                                        min = Integer.parseInt(elements[index]);
                                    }
                                } else {
                                    if (min <= Integer.parseInt(elements[index])) {
                                        min = Integer.parseInt(elements[index]);
                                    }
                                }
                            } else {
                                if (ascOrDesc.equalsIgnoreCase("asc")) {
                                    if (stringComparisonMin > stringToAdd.compareTo(elements[index]) && stringToAdd.compareTo(elements[index])>0) {
                                        stringComparisonMin = stringToAdd.compareTo(elements[index]);
                                        stringToAdd = elements[index];
                                    }
                                } else {
                                    if (stringComparisonMin <= elements[index].compareTo(stringToAdd)) {
                                        stringComparisonMin = elements[index].compareTo(stringToAdd);
                                        stringToAdd = elements[index];
                                    }
                                }
                            }
                        }
                    } else {
                        try {
                            closeAndDeleteFile(readers, k);
                        } catch (IndexOutOfBoundsException ignored) {
                            System.out.println("?");
                        }
                    }
                }
                count = 0;
                //Writes every value which is == to min and continues on.
                for (int j = 0; j < slices; j++) {

                    if (firstLines[j] != null) {
                        elements = firstLines[j].split(",");
                        // This will be only for integers. Need something for strings
                        for (int i = 0; i < intOrNot.length; i++) {
                            if (firstLines[j] == null) {
                                break;
                            }
                            int index = userSortDecisionIndexes[i];
                            if (intOrNot[i]) {
                                if (min == Integer.parseInt(elements[index])) {
                                    writer.write(firstLines[j]);
                                    writer.newLine();
                                    try {
                                        firstLines[j] = readers.get(j).readLine();
                                    } catch (IOException ignored) {
                                        System.out.println("IO");
                                    }
                                }
                            } else {
                                if (stringToAdd.equals(elements[index])) {
                                    writer.write(firstLines[j]);
                                    writer.newLine();
                                    firstLines[j] = readers.get(j).readLine();
                                }
                            }
                        }
                    } else {
                        closeAndDeleteFile(readers, j);
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

        System.out.printf("TIME: %.2f", (System.nanoTime() - begin) / (Math.pow(10, 9)));
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

    public static void merge(String[] strings, String[] temp, int from, int mid, int to) {
        // Must add comparison logic for different elements => int and strings.
        int k = from, i = from, j = mid + 1;
        while (i <= mid && j <= to) {
            // Logic or method call for comparison of different columns
            String[] leftElement = strings[i].split(",");
            String[] rightElement = strings[j].split(",");

            if (ascOrDesc.equalsIgnoreCase("asc")) {
                /* switch for int or not
                    loop to call method for string or int for different sorting options
                    set temp array with the corresponding val.
                 */
                for (int p = 0; p < userSortDecisionIndexes.length; p++) {
                    if (intOrNot[p]) {
                        if ((Integer.parseInt(leftElement[userSortDecisionIndexes[p]])) < Integer.parseInt(rightElement[userSortDecisionIndexes[p]])) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    } else {
                        if (leftElement[userSortDecisionIndexes[p]].compareTo(rightElement[userSortDecisionIndexes[p]]) < 0) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    }
                }

            } else {
                /* switch for int or not
                    loop to call method for string or int for different sorting options
                 */

                for (int p = 0; p < userSortDecisionIndexes.length; p++) {
                    if (intOrNot[p]) {
                        if ((Integer.parseInt(leftElement[userSortDecisionIndexes[p]])) < Integer.parseInt(rightElement[userSortDecisionIndexes[p]])) {
                            temp[k++] = strings[j++];

                        } else {
                            temp[k++] = strings[i++];
                        }
                    } else {
                        if (leftElement[userSortDecisionIndexes[p]].compareTo(rightElement[userSortDecisionIndexes[p]]) > 0) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    }
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


    public synchronized static void writeSortedFile(String[] elements, int sliceNumber) throws IOException {
        try (FileWriter sortedFile = new FileWriter(String.format("C:\\csv\\sortedFiles\\sortedGeneration%d.csv", sliceNumber)); BufferedWriter writer = new BufferedWriter(sortedFile)) {
            for (String a : elements) {
                writer.write(a);
                writer.newLine();
            }
            System.out.printf("sortedGeneration%d.csv has been created!%n", sliceNumber);
        }
    }

    public static void closeAndDeleteFile(List<BufferedReader> readers, int index) {
        try {
            readers.get(index).close();
            Files.delete(Paths.get(String.format("%s%s%d.csv", tempFileDir, fileNames, index)));
        } catch (NoSuchFileException ignored) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}