import org.example.filereader.ExternalSorter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExternalSortedTests {
    String[] normalArray;
    String[] reversedArray;

    @BeforeEach
    public void initialization() {
        normalArray = new String[]{"Teodor,5", "George,2", "Maria,40", "John,10"};
        reversedArray = new String[]{"5,Teodor", "2,George", "40,Maria", "10,John"};
    }

    @Test
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


    @Test
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

    @Test
    public void mergeSort_should_sort_whereStringsAreFirstAndIntsSecond_inAscendingOrder_byNumber() {
        ExternalSorter.mergesort(normalArray, "asc", 1, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("George,2", normalArray[0]),
                () -> Assertions.assertEquals("Teodor,5", normalArray[1]),
                () -> Assertions.assertEquals("John,10", normalArray[2]),
                () -> Assertions.assertEquals("Maria,40", normalArray[3])
        );
    }

    @Test
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
    @Test
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

    @Test
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

    @Test
    public void mergeSort_should_sort_arrayOfStrings_where_intsAreFirstAndStringsSecond_inAscendingOrder_byNumber() {
        ExternalSorter.mergesort(reversedArray, "asc", 0, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("2,George", reversedArray[0]),
                () -> Assertions.assertEquals("5,Teodor", reversedArray[1]),
                () -> Assertions.assertEquals("10,John", reversedArray[2]),
                () -> Assertions.assertEquals("40,Maria", reversedArray[3])
        );
    }

    @Test
    public void mergeSort_should_sort_arrayOfStrings_where_intsAreFirstAndStringsSecond_inDescendingOrder_byNumber() {
        ExternalSorter.mergesort(reversedArray, "desc", 0, true);

        Assertions.assertAll(
                () -> Assertions.assertEquals("40,Maria", reversedArray[0]),
                () -> Assertions.assertEquals("10,John", reversedArray[1]),
                () -> Assertions.assertEquals("5,Teodor", reversedArray[2]),
                () -> Assertions.assertEquals("2,George", reversedArray[3])
        );
    }
}
