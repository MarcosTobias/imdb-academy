package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.JsonpMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.stream.JsonParser;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadDataUtils {
    private static final ElasticsearchClient client = ElasticUtils.getClient();

    public static void getData(String filePath) {
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String currentLine = reader.readLine();

            while(currentLine != null) {
                Map<String, JsonObject> bulkOps = new HashMap<>();
                for(int i = 0; i < 1000; i++) {
                    currentLine = reader.readLine();

                    if(currentLine != null)
                        bulkOps.putAll(getJson(currentLine));
                    else
                        break;
                }


                index(bulkOps);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }


    private static Map<String, JsonObject> getJson(String line) {
        String[] fields = line.split("\t");
        Map<String, JsonObject> element = new HashMap<>();


        element.put(fields[0], Json.createObjectBuilder()
                .add("titleType", fields[1])
                .add("primaryTitle", fields[2])
                .add("originalTitle", fields[3])
                .add("isAdult", StringToBoolConverter.getBool(fields[4]))
                .add("startYear", StringToIntConverter.getInt(fields[5]))
                .add("endYear", StringToIntConverter.getInt(fields[6]))
                .add("runtimeMinutes", StringToIntConverter.getInt(fields[7]))
                .add("genres", fields[8])
                .build());

        return element;
    }
    private static void index(Map<String, JsonObject> bulkOps) {
        try {
            client.bulk(_0 -> _0
                    .operations(bulkOps.keySet().stream().map(x ->
                            BulkOperation.of(_1 -> _1
                                    .index(_2 -> _2
                                            .index("films")
                                            .id(x)
                                            .document(bulkOps.get(x))
                                    )
                            )
                    ).toList())
            );
            //client.index(builder -> builder.index("films").id(fields[0]).withJson(queryJson));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
