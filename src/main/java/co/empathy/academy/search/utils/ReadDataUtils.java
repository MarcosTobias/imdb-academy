package co.empathy.academy.search.utils;

import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Used for reading data from the file specified on batches
 */
public class ReadDataUtils {
    //Size of the batches
    private int batchSize = 5000;

    //Reader for the file
    private BufferedReader reader = null;

    //Determines if the file has been read entirely
    private boolean ready = true;

    /**
     * Creates a ReadDataUtils object
     * @param filePath of the file that is wanted to be read
     * @param batchSize size of the batches to be used
     * @throws FileNotFoundException in case the file specified is not found
     */
    public ReadDataUtils(String filePath, int batchSize) throws FileNotFoundException {
        this(filePath);
        this.batchSize= batchSize;
    }

    /**
     * Creates a ReadDataUtils object
     * @param filePath of the file that is wanted to be read
     * @throws FileNotFoundException in case the file specified is not found
     */
    public ReadDataUtils(String filePath) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(filePath));

        //Skips the first line with the name of the columns
        skipFirstLine();
    }

    /**
     * Skips the first line of the file
     */
    private void skipFirstLine() {
        try {
            reader.readLine();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns a map consisting on the id of the specified line, and the value
     * being a JSON with the different fields of the line parsed
     * @param line containing the document that we want to index
     * @return a Map with the id and the document in JSON format
     */
    private Map<String, JsonObject> getJson(String line) {
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

    /**
     * Returns if the document has been read entirely
     * @return boolean
     */
    public boolean ready() {
        return this.ready;
    }

    /**
     * Returns a map containing as many documents as specified in the batchSize.
     * @return map containind the ids and documents in JSON format
     */
    public Map<String, JsonObject> getDataBatch() {
        //iterator
        int counter = 0;

        try {
            //map that is going to be returned
            Map<String, JsonObject> bulkOps = new HashMap<>();

            //Adding batchSize documents
            while(counter < batchSize) {
                //Read the current line
                String currentLine = reader.readLine();

                //If the line is not null we add it to the map
                if (currentLine != null) {
                    bulkOps.putAll(getJson(currentLine));

                //If the line is null, then we must mark the document as read and stop
                } else {
                    this.ready = false;
                    break;
                }

                counter++;
            }

            return bulkOps;
        } catch(IOException e) {
            e.printStackTrace();
        }

        //In case the file provided only has one line that has been skipped
        return Collections.emptyMap();
    }
}
