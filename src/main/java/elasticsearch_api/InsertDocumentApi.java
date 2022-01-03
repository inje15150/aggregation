package elasticsearch_api;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

public class InsertDocumentApi {

    static final Logger log = LogManager.getLogger(InsertDocumentApi.class);
    private final static String OK = "OK";
    private final static String ERROR = "ERROR";
    private final static String NOT_FOUND = "NOT_FOUND";
    private final static String INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR";

    public void insert(RestHighLevelClient client, Map<String, Object> map, String indexName, String typeName, String nodeName, String fieldName, String id, String dateFieldName) throws IOException {

        IndexCreateApi indexCreateApi = new IndexCreateApi(client);

        // update 시 마다 field name 바뀌기 때문에 새로운 필드 매핑 추가
        indexCreateApi.indexMappings(new PutMappingRequest(indexName).type(typeName), typeName, fieldName, id, nodeName);

        IndexRequest request = new IndexRequest(indexName, typeName); // request 객체 생성

        Gson gson = new Gson();
        request.source(gson.toJson(map), XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);// request document create
        log.info("Send document creation query.");

        int status = response.status().getStatus();
        if (status == 201) {
            log.info("[{}] [{}/{}/{}] : Insert document successfully !", OK, indexName, typeName, nodeName);
        } else if (status == 404) {
            log.error("[{}] [{}]/Status:[{}] error.[{}]", ERROR, NOT_FOUND, status, nodeName);
            throw new ElasticsearchStatusException("Elasticsearch response status error.", RestStatus.NOT_FOUND);
        } else if (status == 500) {
            log.error("[{}] [{}]/Status:[{}] error.[{}]", ERROR, INTERNAL_SERVER_ERROR, status, nodeName);
            throw new ElasticsearchStatusException("Elasticsearch response status error.", RestStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
