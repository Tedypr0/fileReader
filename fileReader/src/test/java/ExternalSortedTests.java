import org.example.filereader.ExternalSorter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.*;

public class ExternalSortedTests {
    String[] normalArray;
    String[] reversedArray;
    String sortedFileDir = "C:\\csv\\sortedFiles\\result.csv";
    @Mock
    ExternalSorter externalSorter;
    @BeforeEach
    public void initialization() {
        normalArray = new String[]{"Teodor,5", "George,2", "Maria,40", "John,10"};
        reversedArray = new String[]{"5,Teodor", "2,George", "40,Maria", "10,John"};
        externalSorter = new ExternalSorter();
    }

    @RepeatedTest(1000)
    public void mergeSort_should_sort_arrayOfStrings_whereStringsAreFirstAndIntsSecond_inAscendingOrder_byString() {
        //Arrange, Act
        ExternalSorter.mergesort(normalArray, "asc", 0, false);

        //Assert
        Assertions.assertAll(
                () -> Assertions.assertEquals("George,2", normalArray[0]),
                () -> Assertions.assertEquals("John,10", normalArray[1]),
                () -> Assertions.assertEquals("Maria,40", normalArray[2]),
                () -> Assertions.assertEquals("Teodor,5", normalArray[3])
        );
    }


    @RepeatedTest(1000)
    public void mergeSort_should_sort_whereStringsAreFirstAndIntsSecond_inDescendingOrder_byString() {
        //Arrange, Act
        ExternalSorter.mergesort(normalArray, "desc", 0, false);

        //Assert
        Assertions.assertAll(
                () -> Assertions.assertEquals("Teodor,5", normalArray[0]),
                () -> Assertions.assertEquals("Maria,40", normalArray[1]),
                () -> Assertions.assertEquals("John,10", normalArray[2]),
                () -> Assertions.assertEquals("George,2", normalArray[3])
        );
    }

    @RepeatedTest(1000)
    public void mergeSort_should_sort_whereStringsAreFirstAndIntsSecond_inAscendingOrder_byNumber() {
        ExternalSorter.mergesort(normalArray, "asc", 1, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("George,2", normalArray[0]),
                () -> Assertions.assertEquals("Teodor,5", normalArray[1]),
                () -> Assertions.assertEquals("John,10", normalArray[2]),
                () -> Assertions.assertEquals("Maria,40", normalArray[3])
        );
    }

    @RepeatedTest(1000)
    public void mergeSort_should_sort_arrayOfStrings_where_whereStringsAreFirstAndIntsSecond_inDescendingOrder_byNumber() {
        ExternalSorter.mergesort(normalArray, "desc", 1, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("Maria,40", normalArray[0]),
                () -> Assertions.assertEquals("John,10", normalArray[1]),
                () -> Assertions.assertEquals("Teodor,5", normalArray[2]),
                () -> Assertions.assertEquals("George,2", normalArray[3])
        );
    }

    //Reversed array tests
    @RepeatedTest(1000)
    public void mergeSort_should_sort_arrayOfStrings_where_intsAreFirstAndStringsSecond_inAscendingOrder_byString() {
        //Arrange, Act
        ExternalSorter.mergesort(reversedArray, "asc", 1, false);

        //Assert
        Assertions.assertAll(
                () -> Assertions.assertEquals("2,George", reversedArray[0]),
                () -> Assertions.assertEquals("10,John", reversedArray[1]),
                () -> Assertions.assertEquals("40,Maria", reversedArray[2]),
                () -> Assertions.assertEquals("5,Teodor", reversedArray[3])
        );
    }

    @RepeatedTest(1000)
    public void mergeSort_should_sort_arrayOfStrings_where_intsAreFirstAndStringsSecond_inDescendingOrder_byString() {
        //Arrange, Act
        ExternalSorter.mergesort(reversedArray, "desc", 1, false);

        //Assert
        Assertions.assertAll(
                () -> Assertions.assertEquals("5,Teodor", reversedArray[0]),
                () -> Assertions.assertEquals("40,Maria", reversedArray[1]),
                () -> Assertions.assertEquals("10,John", reversedArray[2]),
                () -> Assertions.assertEquals("2,George", reversedArray[3])
        );
    }

    @RepeatedTest(1000)
    public void mergeSort_should_sort_arrayOfStrings_where_intsAreFirstAndStringsSecond_inAscendingOrder_byNumber() {
        ExternalSorter.mergesort(reversedArray, "asc", 0, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("2,George", reversedArray[0]),
                () -> Assertions.assertEquals("5,Teodor", reversedArray[1]),
                () -> Assertions.assertEquals("10,John", reversedArray[2]),
                () -> Assertions.assertEquals("40,Maria", reversedArray[3])
        );
    }

    @RepeatedTest(1000)
    public void mergeSort_should_sort_arrayOfStrings_where_intsAreFirstAndStringsSecond_inDescendingOrder_byNumber() {
        ExternalSorter.mergesort(reversedArray, "desc", 0, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("40,Maria", reversedArray[0]),
                () -> Assertions.assertEquals("10,John", reversedArray[1]),
                () -> Assertions.assertEquals("5,Teodor", reversedArray[2]),
                () -> Assertions.assertEquals("2,George", reversedArray[3])
        );
    }

    // Test for the following data arranged in any way should produce the result inside of the array.
    @RepeatedTest(100000)
    public void stressTest_should_sort_arrayOfStrings_consistently_byName_inAscendingOrder() throws IOException {
        String demoInput = "name,asc";
        InputStream in = new ByteArrayInputStream(demoInput.getBytes());
        System.setIn(in);
        externalSorter.start();
        String[] arr = {"name,age",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaA,123",
                "DarleneFStoops@armyspy.com,51",
        "ElnoraEStevens@rhyta.com,6",
        "GeorgeGHise@einrot.com,7",
        "KennethDGarcia@cuvox.de,7",
        "LeighCMeneses@superrito.com,19",
        "MarcEKey@gustr.com,15",
        "NellieRSimmon@jourrapide.com,31",
        "RichardRSchrum@einrot.com,88",
        "RobertCRosario@rhyta.com,2",
        "SamanthaEMerry@superrito.com,56",
        "StevenCMiller@teleworm.us,1",
        "StevenCMiller@teleworm.us,1",};

        BufferedReader bufferedReader = new BufferedReader(new FileReader(sortedFileDir));
        for (String s : arr) {
            Assertions.assertEquals(s, bufferedReader.readLine().replaceAll("\\P{ASCII}", ""));
        }
    }
}
