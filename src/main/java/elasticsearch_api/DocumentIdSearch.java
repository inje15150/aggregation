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
            // 검색
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.types(typeName);

            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());

            searchRequest.source(builder);

            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

            // 응답 값 중에 hits 값 리스트로 추출
            SearchHit[] hits = search.getHits().getHits();

            // 각 도큐먼트 정보 iterator
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();

                String id = (String) sourceAsMap.get("id"); // id 필드 값 추출
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
}
