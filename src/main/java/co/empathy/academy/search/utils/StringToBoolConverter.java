package co.empathy.academy.search.utils;

public class StringToBoolConverter {
    public static boolean getBool(String content) {
        return content.contentEquals("0");
    }
}
