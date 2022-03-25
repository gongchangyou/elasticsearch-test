import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.braindata.api.ApiApplication;
import com.braindata.api.model.User;
import com.fasterxml.jackson.databind.util.JSONPObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/3/15 9:41 上午
 */

@Slf4j
@SpringBootTest(classes= ApiApplication.class)
public class ElasticSearch {

    //es 客户端
    private ElasticsearchClient client;

    @PostConstruct
    public void init() {
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost("10.10.48.194", 9200)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
    }

    @Test
    public void test() {
        System.out.println("hello world");

        SearchResponse<User> search = null;
        try {
            search = client.search(s -> s
                            .index("users")
                            .query(q -> q
                                    .term(t -> t
                                            .field("age")
                                            .value(v -> v.stringValue("32"))
                                    )),
                    User.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Hit<User> hit: search.hits().hits()) {
            log.info(hit.source().toString());
        }
    }
}