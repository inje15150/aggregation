package elasticsearch_api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DocumentIdSearch {

    private final static Logger log = LogManager.getLogger(DocumentIdSearch.class);

    public List<Map<String, String>> getDocumentId(RestHighLevelClient client, String indexName, String typeName) {

        // 도큐먼트들 정보를 담을 Map 형태의 List 생성
        // {nodeName : documentId} 형태의 리스트
        List<Map<String, String>> docs = new ArrayList<>();
        try {
            SearchHit[] hits = response(client, indexName, typeName);

            // 각 도큐먼트 정보 iterator
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();

                String id = (String) sourceAsMap.get("node_name"); // id 필드 값 추출
                String documentId = hit.getId(); // 도큐먼트 id 추출
                Map<String, String> idMap = new ConcurrentHashMap<>();
                idMap.put(id, documentId);
                docs.add(idMap);
            }
            log.info("Add to list by matching node and document id.");

            return docs;
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return docs;
    }

    public boolean isFieldContain(RestHighLevelClient client, String indexName, String typeName, String dateFieldName) throws IOException {
        boolean existFieldName = false;
        SearchHit[] hits = response(client, indexName, typeName);

        for (SearchHit hit : hits) {
             existFieldName = hit.getSourceAsMap().containsKey(dateFieldName);
        }
        return existFieldName;
    }

    public String getKey(RestHighLevelClient client, String indexName, String typeName, String ip) throws IOException {

        String key = null;
        SearchResponse response = keyResponse(client, indexName, typeName, ip);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            String ipAddress = (String) hit.getSourceAsMap().get("ip");
            String macAddress = (String) hit.getSourceAsMap().get("mac_address");
            String gateway = (String) hit.getSourceAsMap().get("gateway");

            key = ipAddress + "%" + macAddress + "%" + gateway;
        }
        return key;
    }

    // Document Search 에 대한 응답 값 얻어오기
    public SearchHit[] response(RestHighLevelClient client, String indexName, String typeName) throws IOException {

        // 검색
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(builder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        // 응답 값 중에 hits 값 리스트로 리턴
        return response.getHits().getHits();
    }



    // Document Search 에 대한 응답 값 얻어오기
    public SearchResponse keyResponse(RestHighLevelClient client, String indexName, String typeName, String ip) throws IOException {

        // 검색
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(1)
                .query(QueryBuilders
                        .boolQuery()
                        .filter(QueryBuilders
                                .termQuery("ip", ip))
                );

        searchRequest.source(builder);

        // 응답 값 리턴
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }
}
