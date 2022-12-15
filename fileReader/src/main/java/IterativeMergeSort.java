import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class IterativeMergeSort {
    public static void merge(String[] strings, String[] temp, int from, int mid, int to, String sortBy, String ascDesc) {
        int k = from, i = from, j = mid + 1;

        while (i <= mid && j <= to) {
            String[] leftElement = strings[i].split(",");
            String[] rightElement = strings[j].split(",");
            if (sortBy.equalsIgnoreCase("country")) {
                if (ascDesc.equalsIgnoreCase("asc")) {
                    if ((leftElement[0].compareTo(rightElement[0])) < 0) {
                        temp[k++] = strings[i++];
                    } else {
                        temp[k++] = strings[j++];
                    }
                } else {
                    if ((leftElement[0].compareTo(rightElement[0])) > 0) {
                        temp[k++] = strings[i++];
                    } else {
                        temp[k++] = strings[j++];
                    }
                }
            } else {
                if (sortBy.equalsIgnoreCase("state")) {
                    if (ascDesc.equalsIgnoreCase("asc")) {
                        if ((leftElement[1].compareTo(rightElement[1])) < 0) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    } else {
                        if ((leftElement[1].compareTo(rightElement[1])) > 0) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    }
                } else if (sortBy.equalsIgnoreCase("username")) {
                    if (ascDesc.equalsIgnoreCase("asc")) {
                        if ((leftElement[2].compareTo(rightElement[2])) < 0) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    } else {
                        if ((leftElement[2].compareTo(rightElement[2])) > 0) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    }

                } else if (sortBy.equalsIgnoreCase("age")) {
                    if (ascDesc.equalsIgnoreCase("asc")) {
                        if ((Integer.parseInt(leftElement[8])) < Integer.parseInt(rightElement[8])) {
                            temp[k++] = strings[i++];
                        } else {
                            temp[k++] = strings[j++];
                        }
                    } else {
                        if ((Integer.parseInt(leftElement[8])) > Integer.parseInt(rightElement[8])) {
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

    public static void mergesort(String[] str, String sortBy, String ascDesc) {
        int low = 0;
        int high = str.length - 1;

        String[] temp = Arrays.copyOf(str, str.length);
        for (int m = 1; m <= high - low; m = 2 * m) {
            for (int i = low; i < high; i += 2 * m) {
                int mid = i + m - 1;
                int to = Integer.min(i + 2 * m - 1, high);
                merge(str, temp, i, mid, to, sortBy, ascDesc);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String[] res = new String[1000005];
        long beforeBefore = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new java.io.FileReader("C:\\generation.csv"));
        String cols = reader.readLine();
        String line;
        int k = 0;
        while ((line = reader.readLine()) != null) {
            res[k] = line;
            k++;
        }
        long after = System.currentTimeMillis()-beforeBefore;
        Scanner sc = new Scanner(System.in);
        String cmd = "";
        String sortBy = "";
        long before = 0;
        while (cmd.equalsIgnoreCase("") && sortBy.equalsIgnoreCase("")) {
            System.out.println("Sort by country, state, username, age");
            cmd = sc.nextLine();
            System.out.println("Sort ascending or descending. asc or desc");
            sortBy = sc.nextLine();
            before= System.currentTimeMillis();
            switch (cmd.toLowerCase()) {
                case "country":
                    mergesort(res, "country", sortBy.equalsIgnoreCase("asc") ? "asc" : "desc");
                    break;
                case "state":
                    mergesort(res, "state", sortBy.equalsIgnoreCase("asc") ? "asc" : "desc");
                    break;
                case "username":
                    mergesort(res, "username", sortBy.equalsIgnoreCase("asc") ? "asc" : "desc");
                    break;
                case "age":
                    mergesort(res, "age", sortBy.equalsIgnoreCase("asc") ? "asc" : "desc");
                    break;
                default:
                    System.out.println("Unknown sortBy!");
                    break;
            }
        }
        System.out.printf("You have successfully sorted by %s in %s order\n", cmd, sortBy);
        try (
                FileWriter sortedFile = new FileWriter("sortedGeneration.csv");
                BufferedWriter writer = new BufferedWriter(sortedFile)
        ) {
            writer.write(cols);
            writer.newLine();
            for (String a : res) {
                writer.write(a);
                writer.newLine();
            }
        }
        System.out.println("Read, sort and write time: ");
        System.out.println(System.currentTimeMillis() - before+after);
    }
}
