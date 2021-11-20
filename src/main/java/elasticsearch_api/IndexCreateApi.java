package elasticsearch_api;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import java.io.IOException;

@Getter
@Setter
public class IndexCreateApi {

    private RestHighLevelClient client = null;
    private final static Logger log = LogManager.getLogger(IndexCreateApi.class);
    private final String[] FIELD_NAME = {"node1", "node2", "node3"};
    private final String[] INNER_FIELD_NAME = {"cpu", "memory"};
    private CreateIndexRequest indexRequest;
    private PutMappingRequest putMappingRequest;
    private final static String OK = "OK";
    private final static int SHARDS = 5;
    private final static int REPLICA = 1;

    public IndexCreateApi(RestHighLevelClient client) {
        this.client = client;
    }

    //Create Index
    public void createIndex(String indexName, String typeName, String fieldName) {
        indexRequest = new CreateIndexRequest(indexName); // index 생성 객체
        putMappingRequest = new PutMappingRequest(indexName); // mapping 객체 생성
        putMappingRequest.type(typeName);

        indexSettings(indexRequest);
        log.info("[{}/{}] index settings complete.", indexName, typeName);
        log.info("settings : shards : {}, replica : {}", SHARDS, REPLICA);

//        indexMappings(putMappingRequest, typeName, fieldName);// 인덱스 필드 매핑
//        log.info("index mapping complete.");
    }

    //Settings
    public void indexSettings(CreateIndexRequest indexRequest) {
        try {
            indexRequest.settings(Settings.builder()
                    .put("index.number_of_shards", SHARDS)
                    .put("index.number_of_replicas", REPLICA)
                    .put("index.mapping.total_fields.limit", 10000));
            client.indices().create(indexRequest, RequestOptions.DEFAULT);// 인덱스 세팅 쿼리 전달

        } catch (IOException e) {
            log.info(e.getMessage());
        }
    }

    //Mapping
    public void indexMappings(PutMappingRequest putMappingRequest, String typeName, String fieldName, String id, String node_name, String dateFieldName) {
        try {
            XContentBuilder builder = null;

            builder = XContentFactory.jsonBuilder();
            builder.startObject()
                    .startObject(typeName)
                        .startObject("properties")
                            .startObject(id)
                                .field("type", "keyword")
                            .endObject()
                            .startObject(node_name)
                                .field("type", "text")
                            .endObject()
                            .startObject(dateFieldName)
                                .startObject("properties")
                                    .startObject(fieldName)
                                        .startObject("properties")// type name
                                            .startObject("event_time")
                                                .field("type", "date")
                                                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy/MM/dd HH:mm:ss||epoch_millis")
                                            .endObject();

            for (String inner_field : INNER_FIELD_NAME) { // node 별 field name
                builder.startObject(inner_field)
                        .startObject("properties")
                            .startObject("count")
                                .field("type", "long")
                            .endObject()
                            .startObject("sum")
                                .field("type", "float")
                            .endObject()
                            .startObject("avg")
                                .field("type", "float")
                            .endObject()
                            .startObject("max")
                                .field("type", "float")
                            .endObject()
                            .startObject("min")
                                .field("type", "float")
                            .endObject()
                        .endObject()
                        .endObject();
            }
            builder.endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();

            putMappingRequest.source(builder); // mapping 쿼리 source(body) 에 담기
            PutMappingResponse response = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);// 실제 mapping 쿼리 전달
            if (response.isAcknowledged()) {
                log.info("[{}] Document new field mapping success.", OK);
            } else {
                throw new IOException();
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }


    // 인덱스 존재 여부
    public boolean existIndex(String indexName) {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indexName);

        boolean acknowledged = false;

        try {
            acknowledged = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info(e.getMessage());
        }
        return acknowledged;
    }
}
