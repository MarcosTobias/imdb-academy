package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.empathy.academy.search.utils.ElasticUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Query controller", description = "Allows to perform queries on the IMDB database")
@RestController
@RequestMapping("/api")
public class QueryController {
    private final ElasticsearchClient client = ElasticUtils.getClient();

    /**
     * Returns the name of the indices currently stored on the elastic container
     * @return Json with the names
     */
    @Operation(summary = "Performs a query on the database")
    @ApiResponse(responseCode = "200", description="Documents obtained", content = { @Content(mediaType= "application/json")})
    @GetMapping("/q")
    public String getIndices() {
        return "test";
    }
}
