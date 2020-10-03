package com.qf.j2003;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


@RunWith(SpringRunner.class)
@SpringBootTest
public class J2003SpcEs7ApplicationTests {
    /**
     * 1、添加index
     */
    @Test
    public void test1() throws IOException {
//        创建http主机对象（es服务器对象）
        HttpHost httpHost = new HttpHost("127.0.0.1",9200);
//        创建es-Rest-client对象
        RestClientBuilder builder = RestClient.builder(httpHost);
//      创建high-level-client对象
        RestHighLevelClient levelClient = new RestHighLevelClient(builder);
//        创建一个请求对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("j2003a");
//        如果创建索引时需要指定索引参数
        createIndexRequest.settings(Settings
                .builder()
                .put("number_of_shards",5)
                .put("number_of_replicas",2)
        );

//     使用levelpclient发送创建索引的请求，并获取响应结果
        CreateIndexResponse createIndexResponse = levelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.toString());
    }

//    2创建带mapping的索引
    
    @Test
    public void createIndexAndMapping() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
        CreateIndexRequest j2003b = new CreateIndexRequest("j2003b");
        j2003b.settings(Settings.builder()
                .put("number_of_shards",5)
                .put("number_of_replicas",2)
        );
        XContentBuilder json = JsonXContent.contentBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name").field("type", "text").endObject()
                .startObject("age").field("type", "text").endObject()
                .endObject()
                .endObject();
        j2003b.mapping(json);
        CreateIndexResponse createIndexResponse = levelClient.indices().create(j2003b, RequestOptions.DEFAULT);
        System.out.println("ok");
    }
//  3、删除索引a
    @Test
    public void deleteIndex() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("j2003a");
//        deleteRequest.indices();
        AcknowledgedResponse delete = levelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete);

    }
// 4、添加文档
    @Test
    public  void addDoc1() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
//        创建一个添加文档的请求对象
        IndexRequest indexRequest = new IndexRequest("j2003b");
//        设置添加文档的id，如果没设置则系统自动生成
        indexRequest.id("1");
//        构造一个对象集合(使用实体对象)
        HashMap<Object, Object> map = new HashMap<>();
          map.put("name","zhangsan");
          map.put("age",23);
//          将对象集合转为json字符串
        String toJson = new Gson().toJson(map);
//        将添加json字符串加如文档请求中
        indexRequest.source(toJson, XContentType.JSON);
//  使用client将文档请求添加到es中
        IndexResponse index = levelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index);

    }
    // 5、添加文档
    @Test
    public  void addDoc2() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
//        创建一个添加文档的请求对象
        IndexRequest indexRequest = new IndexRequest("j2003b");

//        设置添加文档的id，如果没设置则系统自动生成
        indexRequest.id("2");
//创建一个json对象的构建器
        XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .field("name", "acbd")
                .field("age", "23")
                .endObject();
       String str="{\"name\":\"zhangsan\",\"age\":22}";
      indexRequest.source(str,XContentType.JSON);
        indexRequest.source(builder);
        IndexResponse index = levelClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(index);

    }
//    6、查询文档
    @Test
    public void  queryObjectById() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
        GetRequest getRequest = new GetRequest("j2003b","2");
        GetResponse documentFields = levelClient.get(getRequest, RequestOptions.DEFAULT);
//        从相应结果中，提取属性集合
        Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
        Iterator<String> it = sourceAsMap.keySet().iterator();
        while(it.hasNext()){
            String k = it.next();
            Object v = sourceAsMap.get(k);
            System.out.println("key:"+k+" v:"+v);
        }
    }
//    7删除文档
    @Test
    public void deleteDoc() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
        DeleteRequest deleteRequest = new DeleteRequest();
            deleteRequest.index("j2003b");
            deleteRequest.id("2");

        DeleteResponse delete = levelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }
//    8批量添加文档
    @Test
    public void bulkaddDoc() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
        BulkRequest bulkRequest = new BulkRequest();
//        给bulk请求添加文档
        HashMap map1 = new HashMap();
        map1.put("name","wangwu");
        map1.put("age",25);
        String json1 = new Gson().toJson(map1);
        bulkRequest.add(new IndexRequest("j2003b")
                .id("3")
                .source(json1,XContentType.JSON)
        ) ;
//        给bulk请求添加文档
        HashMap map2 = new HashMap();
        map2.put("name","maliu");
        map2.put("age",30);
        String json2 = new Gson().toJson(map2);
        bulkRequest.add(new IndexRequest("j2003b")
                .id("4")
                .source(json2,XContentType.JSON)
        ) ;

        BulkResponse bulk = levelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk);
    }
//9 查询高亮显示
    @Test
    public  void highLight() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200)));
//        创建查询请求
        SearchRequest searchRequest = new SearchRequest("lib4");
//        创建查询请求构建器
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        追加查询条件
        sourceBuilder.query(QueryBuilders.matchQuery("interests","java"));
//        创建高亮构建器
        HighlightBuilder highlightBuilder = new HighlightBuilder();
           highlightBuilder.field("interests",12);
           highlightBuilder.preTags("<font color='red'>");
           highlightBuilder.postTags("</font>");
//           装配查询构建器（高亮）
        sourceBuilder.highlighter(highlightBuilder);
//        给查询对象添加含有高亮、匹配条件的查询条件对象（构建器）
        searchRequest.source(sourceBuilder);

        SearchResponse search = levelClient.search(searchRequest, RequestOptions.DEFAULT);
//
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit: hits    ) {
            String v = hit.getHighlightFields().get("interests").toString();
            System.out.println(v);
        }
    }
}
