package org.example;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExternalSorter {
    static int lines = 500000;
    static int maxElements = 100000;
    static String fileNames = "sortedGeneration";

    public static void main(String[] args) throws IOException {
        int slices = (int) Math.ceil((double) lines / maxElements);
        try (FileReader fileToSort = new FileReader("C:\\csv\\500k.csv");
             BufferedReader buffer = new BufferedReader(fileToSort)) {
            String line;
            line = buffer.readLine();
            String[] elements = new String[maxElements];
            int sliceNumber = 0;
            for (int i = 0; i < slices; i++) {
                for (int j = 0; j < maxElements; j++) {
                    line = buffer.readLine();
                    if (line != null) {
                        elements[j] = line;
                    } else {
                        break;
                    }
                }
                mergesort(elements);
                //Write slices
                writeSortedFile(elements, sliceNumber);
                sliceNumber++;
                elements = new String[maxElements];
            }
        } catch (IOException e) {
            System.out.println("File not found");
        }
        List<BufferedReader> readers = new ArrayList<>(slices);
        String[] firstLines = new String[slices];
        String line;
        for (int i = 0; i < slices; i++) {
            readers.add(new BufferedReader(new FileReader(String.format("C:\\csv\\sortedFiles\\%s%d.csv", fileNames, i))));
            line = readers.get(i).readLine();
            firstLines[i] = line;
        }
        int min;
        String[] elements;
        BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\csv\\sortedFiles\\result.csv"));
        //Reads all Files
        for (int i = 0; i < maxElements; i++) {
            min = Integer.MAX_VALUE;
            //Reads finds min value for each reader
            for (int k = 0; k < slices; k++) {
                elements = firstLines[k].split(",");
                if (min >= Integer.parseInt(elements[1])) {
                    min = Integer.parseInt(elements[1]);
                }

            }

            //Writes every value which is == to min and continues onward.
            for (int j = 0; j < slices; j++) {
                elements = firstLines[j].split(",");
                if (min <= Integer.parseInt(elements[1])) {
                    writer.write(firstLines[j]);
                    writer.newLine();
                    firstLines[j] = readers.get(j).readLine();
                }

            }
        }
    }

    public static void merge(String[] strings, String[] temp, int from, int mid, int to) {
        int k = from, i = from, j = mid + 1;
        try {
            while (i <= mid && j <= to) {
                String[] leftElement = strings[i].split(",");
                String[] rightElement = strings[j].split(",");
                if ((Integer.parseInt(leftElement[1])) < Integer.parseInt(rightElement[1])) {
                    temp[k++] = strings[i++];
                } else {
                    temp[k++] = strings[j++];
                }
            }
        } catch (NullPointerException e) {
            System.out.println("NULL");
        } catch (ArrayIndexOutOfBoundsException b) {
            System.out.println("?");
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


    public static void writeSortedFile(String[] elements, int sliceNumber) throws IOException {
        try (
                FileWriter sortedFile = new FileWriter(String.format("C:\\csv\\sortedFiles\\sortedGeneration%d.csv", sliceNumber));
                BufferedWriter writer = new BufferedWriter(sortedFile)
        ) {
            for (String a : elements) {
                writer.write(a);
                writer.newLine();
            }
        }
    }
}

;