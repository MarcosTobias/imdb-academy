package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.search.exception.types.InternalServerException;
import co.empathy.academy.search.utils.clases.Film;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Used for reading data from the file specified on batches
 */
public class ReadDataUtils {
    //Size of the batches
    private static final int BATCH_SIZE = 25000;
    private static final Logger logger = LoggerFactory.getLogger(ReadDataUtils.class);
    private final ElasticsearchClient client = ElasticUtils.getClient();

    /**
     * Index the data
     */
    public void indexData(String filmsPath) {
        try {
            logger.info("Started indexing");
            List<String> lines = Files.readAllLines(Path.of(filmsPath));
            List<String> headers = Arrays.stream(lines.get(0).split("\t")).toList();

            logger.info("Read");

            long current = 1;
            long localBatch = BATCH_SIZE;

            if(BATCH_SIZE > lines.size()) localBatch = lines.size() - 1L;

            while(current < lines.size() - 1L) {
                if (current + BATCH_SIZE > lines.size())
                    localBatch = lines.size() - current;

                long finalCurrent = current;
                long finalLocalBatch = localBatch;

                client.bulk(_0 -> _0
                        .operations(lines.stream().skip(finalCurrent).limit(finalLocalBatch)
                                .map(x -> Film.toJsonContent(x, headers))
                                .map(x ->
                                        BulkOperation.of(_1 -> _1
                                                .index(_2 -> _2
                                                        .index("films")
                                                        .document(x.json())
                                                        .id(x.id())
                                                )
                                        )
                                ).toList())
                );


                logger.info("Indexed");
                current += BATCH_SIZE;
            }
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }
}
