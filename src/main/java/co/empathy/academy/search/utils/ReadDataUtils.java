package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.io.*;

public class ReadDataUtils {
    private static final ElasticsearchClient client = ElasticUtils.getClient();

    public static void getData(String filePath) {
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            reader.readLine();
            String currentLine;
            while((currentLine = reader.readLine()) != null) {
                index(currentLine);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }


    private static void index(String line) {
        String[] fields = line.split("\t");

        JsonObject json = Json.createObjectBuilder()
                .add("titleType", fields[1])
                .add("primaryTitle", fields[2])
                .add("originalTitle", fields[3])
                .add("isAdult", StringToBoolConverter.getBool(fields[4]))
                .add("startYear", StringToIntConverter.getInt(fields[5]))
                .add("endYear", StringToIntConverter.getInt(fields[6]))
                .add("runtimeMinutes", StringToIntConverter.getInt(fields[7]))
                .add("genres", fields[8])
                .build();


        Reader queryJson = new StringReader(json.toString());


        try {
            client.index(builder -> builder.index("films").id(fields[0]).withJson(queryJson));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
