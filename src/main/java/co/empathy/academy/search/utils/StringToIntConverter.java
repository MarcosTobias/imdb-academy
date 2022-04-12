package co.empathy.academy.search.utils;

/**
 * Converts a string containing an int or a \N to an Integer
 */
public class StringToIntConverter {
    private StringToIntConverter() {}

    public static int getInt(String content) {
        if(content.contentEquals("\\N")) {
            return 0;
        } else {
            return Integer.parseInt(content);
        }
    }
}
