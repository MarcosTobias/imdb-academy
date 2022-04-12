package co.empathy.academy.search.utils;

/**
 * Converts a string containing 1 or 0 to a boolean
 */
public class StringToBoolConverter {
    private StringToBoolConverter(){}

    public static boolean getBool(String content) {
        return content.contentEquals("0");
    }
}
