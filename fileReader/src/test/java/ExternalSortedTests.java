import org.example.filereader.ExternalSorter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.example.filereader.ExternalSorter.mergesort;

public class ExternalSortedTests {

    @Test
    public void mergeSort_should_sort_ArrayOfStringNumbers(){
        //Arrange
        String[] array = {"Teodor,5", "George,2","Maria,40","John,10"};

        //Act
        mergesort(array);

        //Assert
        Assertions.assertAll(
                () -> Assertions.assertEquals(array[0], "George,2"),
                () -> Assertions.assertEquals(array[1], "Teodor,5"),
                () -> Assertions.assertEquals(array[2], "John,10"),
                () -> Assertions.assertEquals(array[3], "Maria,40")
        );
    }

    @Test
    public void mergeSort_should_throwException_when_GivenString_DoesNotContainComma(){
        String[] array = {"5","2","40", "10"};

        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> mergesort(array));
    }

    @Test
    public void mergeSort_Should_throwException_when_characterAfterCommaIsNot_aNumber(){
        String[] array = {"Teodor,a", "George,b","Maria,v","John,g"};

        Assertions.assertThrows(NumberFormatException.class, () -> mergesort(array));
    }
}
