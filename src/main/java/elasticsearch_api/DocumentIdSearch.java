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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DocumentIdSearch {

    private final static Logger log = LogManager.getLogger(DocumentIdSearch.class);

    public Map<String, String> getDocumentId(RestHighLevelClient client, String indexName, String typeName, String dateFieldName, List<String> keys) {

        // 도큐먼트들 정보를 담을 Map 형태의 List 생성
        // {nodeName : documentId} 형태의 리스트
//        List<Map<String, String>> docs = Collections.synchronizedList(new ArrayList<>());
        Map<String, String> idMap = new ConcurrentHashMap<>();

        try {
            SearchHit[] hits = response(client, indexName, typeName, dateFieldName);

            // 각 도큐먼트 정보 iterator

            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();

                String id = (String) sourceAsMap.get("node_name"); // id 필드 값 추출
                String documentId = hit.getId(); // 도큐먼트 id 추출
                idMap.put(id, documentId);
            }
            log.info("Add to list by matching node and document id.");
            return idMap;

        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return idMap;
    }

    // Document Search 에 대한 응답 값 얻어오기
    public SearchHit[] response(RestHighLevelClient client, String indexName, String typeName, String dateFieldName) throws IOException {

        // 검색
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders
                .boolQuery()
                .filter(QueryBuilders
                        .termQuery("day", dateFieldName)));

        searchRequest.source(builder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        // 응답 값 중에 hits 값 리스트로 리턴
        return response.getHits().getHits();
    }
}
