package aggregation;

import elasticsearch_api.*;
import entity.Total;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregation {

    static final Logger log = LogManager.getLogger(Aggregation.class);
    private final Connection connection = new Connection();
    private final String[] nodeNames = {"node1", "node2", "node3"};
    private final static String OK = "OK";

    public synchronized void aggregation(String index_name, String fieldName, String typeName, String gte, String lt) throws IOException {

        // 인덱스 생성 객체
        IndexCreateApi indexCreateApi = new IndexCreateApi(connection.getClient());

        // 집계 관련 객체 생성
        AggregationApi aggregationApi = new AggregationApi();

        // {node1 : Total}, {node2 : Total}, {node3 : Total} 리턴 값 전달 받음
        // 각 agent 10분 데이터 집계
        Map<String, Total> totals = aggregationApi.aggregation(connection.getClient(), gte, lt, typeName, fieldName);

        // 인덱스 존재하지 않을 시 인덱스 생성
        if (!indexCreateApi.existIndex(index_name)) {
            log.info("index does not exist. So we start creating the index.");
            indexCreateApi.createIndex(index_name, typeName, fieldName);
            log.info("[{}], index creation complete.", OK);
        }

        // 도큐먼트 존재 여부 및 도큐먼트 id 얻어오기 위한 객체 생성
        DocumentIdSearch search = new DocumentIdSearch();
        List<Map<String, String>> docs = search.getDocumentId(connection.getClient(), index_name, typeName);

        Map<String, Object> map = new HashMap<>(); // 최종 데이터 담을 map 생성

        // 도큐먼트가 존재하지 않을 때
        if (docs.isEmpty()) {
            log.info("The document does not exist. insert a document.");
            for (String nodeName : nodeNames) {
                InsertDocumentApi insertDocumentApi = new InsertDocumentApi();
                Total total = totals.get(nodeName);

                Map<String, Total> totalMap = new HashMap<>();
                totalMap.put(fieldName, total);

                map.put("id", nodeName); // {"id" : "식별이름"}
                map.put("data", totalMap);// {"data" : {"00_00" : "Total 데이터"}}
                insertDocumentApi.documentCreate(connection.getClient(), map, index_name, typeName, nodeName);
            }
        } else if (docs.size() == 3) { // 도큐먼트가 존재 할 때
            log.info("[{}] The document already exists. Update the field.", OK);
            for (String nodeName : nodeNames) {
                UpdateDocumentApi updateDocumentApi = new UpdateDocumentApi();
                Total total = totals.get(nodeName); // nodeNam 별 data 필드 값 추출

                Map<String, Total> totalMap = new HashMap<>();
                totalMap.put(fieldName, total);

                map.put("id", nodeName);
                map.put("data", totalMap);

                // 업데이트 시 document id 필요
                for (Map<String, String> doc : docs) {
                    if (doc.containsKey(nodeName)) {
                        String documentId = doc.get(nodeName);
                        updateDocumentApi.updateDocument(index_name, typeName, documentId, map, connection.getClient(), nodeName);
                    }
                }
            }
        } else {
            throw new IOException("The number of documents does not match. check the document.");
        }
    }
}