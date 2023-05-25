package com.example.elasticproject_spring;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.elasticproject_spring.controller.ElasticsearchConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

@SpringBootTest
class ElasticProjectSpringApplicationTests {
    @Autowired
    ElasticsearchConfig elasticsearchConfig;

    @Test
    void create() throws Exception{
        RestClient restClient = RestClient.builder(new HttpHost("192.168.109.128",9200)).setDefaultHeaders(new Header[]{new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String("elastic:39u*ncXQ_nIFzl0JNXdW".getBytes()))}).build();
        // 使用Jackson映射器创建传输层
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );
        // 创建API客户端
        ElasticsearchClient client = new ElasticsearchClient(transport);
//        CreateIndexResponse createIndexResponse = client.indices().create(c -> c.index("user_test"));
        // 响应状态
//        Boolean acknowledged = createIndexResponse.acknowledged();
//        System.out.println("索引操作 = " + acknowledged);
//        System.out.println(client.info());
        //获得索引
//        GetIndexResponse getIndexResponse = client.indices().get(e -> e.index("user_test"));
//        System.out.println("getIndexResponse.result() = " + getIndexResponse.result());
//        System.out.println("getIndexResponse.result().keySet() = " + getIndexResponse.result().keySet());
//        // 关闭ES客户端
//        transport.close();
//        restClient.close();
    }
    @Test
    public void testEs() throws Exception{
        String index="test_index";
        RestHighLevelClient restHighLevelClient = elasticsearchConfig.restHighLevelClient();
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        // 处理获取的索引信息
        int indices = getIndexResponse.getIndices().length;
        // 可以遍历 indices 数组获取每个索引的名称和其他信息
//        for (String x :
//                indices) {
//            System.out.println(x);
//        }
        System.out.println(indices);
        restHighLevelClient.close();
    }

}
