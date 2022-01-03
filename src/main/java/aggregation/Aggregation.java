package aggregation;

import dbconnection.DatabaseCon;
import elasticsearch_api.*;
import entity.Total;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Aggregation {

    static final Logger log = LogManager.getLogger(Aggregation.class);
    private final Connection connection = new Connection();
    private final static String OK = "OK";
    private final static String KEY_GET_INDEX_NAME = "agent_info";

    public synchronized void aggregation(String index_name, String fieldName, String typeName, String gte, String lt, String dateFieldName, String agentInfoTypeName) throws IOException, ElasticsearchStatusException, IllegalArgumentException {

        // 집계 관련 객체 생성
        AggregationApi aggregationApi = new AggregationApi();

        // TODO: 2021-11-21 (DB 연동 후 ipList select)
        DatabaseCon databaseCon = new DatabaseCon();
        databaseCon.connection(); // mariaDB connection

        List<String> nodeIp = databaseCon.selectIpList();

        Map<String, String> nodeIpMap = databaseCon.selectIpAndName();

        // {ip : Total} 리턴 값 전달 받음
        // 각 agent 10분 데이터 집계
        Map<String, Total> totals = aggregationApi.aggregation(connection.getClient(), gte, lt, agentInfoTypeName, fieldName, nodeIp);

        // 인덱스 생성 객체
        IndexCreateApi indexCreateApi = new IndexCreateApi(connection.getClient());

        // 인덱스 존재하지 않을 시 인덱스 생성
        if (!indexCreateApi.existIndex(index_name)) {
            log.info("index does not exist. So we start creating the index.");
            indexCreateApi.createIndex(index_name, typeName);
            log.info("[{}], index creation complete.", OK);
        }

        // 도큐먼트 존재 여부 및 도큐먼트 id 얻어오기 위한 객체 생성
        DocumentIdSearch search = new DocumentIdSearch();


        // <nodeName, documentId> List
        Map<String, String> docs = search.getDocumentId(connection.getClient(), index_name, typeName, dateFieldName, nodeIp);
        log.info("doc size : {}", docs.size());

        Map<String, Object> map = new ConcurrentHashMap<>(); // 최종 데이터 담을 map 생성

        // day 로 검색해온 도큐먼트가 없을 시 insert
        if (docs.isEmpty()) {
            log.info("The document does not exist. insert a document.");
            for (String ip : nodeIp) {

                Total total = totals.get(ip);

                Map<String, String> ipAndKey = databaseCon.selectIpAndKey();
                String key = ipAndKey.get(ip);

                map.put("id", key); // {"id" : "식별이름"}
                map.put("node_name", nodeIpMap.get(ip));
                map.put("day", dateFieldName); // 날짜
                map.put(fieldName, total); // {"00_00" : "Total 데이터"}

                InsertDocumentApi insertDocumentApi = new InsertDocumentApi();

                insertDocumentApi.insert(connection.getClient(), map, index_name, typeName, nodeIpMap.get(ip), fieldName, key, dateFieldName);
            }
        }
        // 도큐먼트가 존재 할 때
        // TODO: 2021-11-17(DB 연동 후 변경 필요)
        // TODO: 2021-11-23
        else if (nodeIp.size() == docs.size()) {
            log.info("[{}] The document already exists. Update the field.", OK);
            for (String ip : nodeIp) {
                Total total = totals.get(ip); // nodeNam 별 data 필드 값 추출

                Map<String, String> ipAndKey = databaseCon.selectIpAndKey();
                String key = ipAndKey.get(ip);

                map.put("id", key);
                map.put("node_name", nodeIpMap.get(ip));
                map.put("day", dateFieldName);
                if (total == null) {
                    total = new Total();
                }
                map.put(fieldName, total);

                UpdateDocumentApi updateDocumentApi = new UpdateDocumentApi();

                // 업데이트 시 document id 필요
                if (docs.containsKey(nodeIpMap.get(ip))) {
                    updateDocumentApi.update(index_name, typeName, fieldName, docs.get(nodeIpMap.get(ip)), map, connection.getClient(), nodeIpMap.get(ip), key);
                }
            }
        }
        databaseCon.close();
    }
}