package co.empathy.academy.search.utils;

public class StringToIntConverter {
    public static int getInt(String content) {
        if(content.contentEquals("\\N")) {
            return 0;
        } else {
            return Integer.parseInt(content);
        }
    }
}
