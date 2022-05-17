package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.search.exception.types.InternalServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Used for reading data from the file specified on batches
 */
public class IndexingUtils {
    //Size of the batches
    private static final int BATCH_SIZE = 25000;
    private static final Logger logger = LoggerFactory.getLogger(IndexingUtils.class);
    private final ElasticsearchClient client = ElasticUtils.getClient();

    /**
     * Index the data
     */
    public void indexData(String filmsPath, String ratingsPath, String akasPath, String crewPath, String episodesPath, String principalPath, String nameBasicsPath) {
        try {
            logger.info("Started indexing");
            var batchReader = new BatchReader(filmsPath, ratingsPath, akasPath, crewPath, episodesPath, principalPath, nameBasicsPath, BATCH_SIZE);

            while(!batchReader.hasFinished()) {
                var batch = batchReader.getBatch();

                client.bulk(_0 -> _0
                    .operations(batch.stream()
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

            }

            logger.info("Indexed");
            batchReader.close();
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }
}
