package elasticsearch_api;

import com.google.gson.Gson;
import date.WantDateCreate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

public class UpdateDocumentApi {
    private final static Logger log = LogManager.getLogger(UpdateDocumentApi.class);
    private final static String OK = "OK";
    private final static String ERROR = "ERROR";
    private final static String NOT_FOUND = "NOT_FOUND";
    private final static String INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR";

    public void updateDocument(String indexName, String typeName, String documentId, Map<String, Object> result, RestHighLevelClient client, String nodeName) throws IOException{

        Gson gson = new Gson();
        WantDateCreate dateCreate = new WantDateCreate();
        IndexCreateApi indexCreateApi = new IndexCreateApi(client);

        // update 시 마다 field name 바뀌기 때문에 새로운 필드 매핑 추가
        indexCreateApi.indexMappings(new PutMappingRequest(indexName).type(typeName), typeName, dateCreate.min_fieldName());

        //update 쿼리 객체 생성
        UpdateRequest request = new UpdateRequest(indexName, typeName, documentId);

        // 업데이트 할 데이터 담기
        request.doc(gson.toJson(result), XContentType.JSON);

        // 실제 update 쿼리 전달
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        log.info("Send document updating query.");

        int status = response.status().getStatus();
        if (status == 200) {
            log.info("[{}] [{}/{}/{}] : Update document successfully !", OK, indexName, typeName, nodeName);
        } else if (status == 404) {
            log.error("[{}] [{}]/Status:[{}] error.[{}]", ERROR, NOT_FOUND, status, nodeName);
            throw new ElasticsearchStatusException("Elasticsearch response status error.", RestStatus.NOT_FOUND);
        } else if (status == 500) {
            log.error("[{}] [{}]/Status:[{}] error.[{}]", ERROR, INTERNAL_SERVER_ERROR, status, nodeName);
            throw new ElasticsearchStatusException("Elasticsearch response status error.", RestStatus.NOT_FOUND);
        }
    }
}