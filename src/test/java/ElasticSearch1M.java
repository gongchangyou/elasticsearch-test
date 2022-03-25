import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.FloatNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.IntegerNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.braindata.api.ApiApplication;
import com.braindata.api.model.Point;
import com.braindata.api.model.User;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpHost;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/3/15 9:41 上午
 */

@Slf4j
@SpringBootTest(classes= ApiApplication.class)
public class ElasticSearch1M {

    //es 客户端
    private ElasticsearchClient client;

    private String index = "map1000000";

    private ThreadPoolExecutor executor = new ThreadPoolExecutor(
            10
            , 10
            , 0L
            , TimeUnit.MILLISECONDS
            , new LinkedBlockingQueue(250)
            , new ThreadPoolExecutor.CallerRunsPolicy()
    );

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
    public void createIndex() {
        Random random = new Random();
        val d = random.nextDouble();//0-1
        try {
            client.indices().create(c -> c.index(index).mappings(
                    new TypeMapping.Builder().properties(new HashMap<>(){{
                        put("x", new Property(new FloatNumberProperty.Builder().build()));
                        put("y", new Property(new FloatNumberProperty.Builder().build()));
                        put("count", new Property(new IntegerNumberProperty.Builder().build()));
                    }}).build()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void importData() {
        Random random = new Random();
        for (int j = 0; j <1000000;j++) { //maybe bulk can batch insert
            final int i = j;
            executor.submit( () -> {
                val x = random.nextDouble();//0-1
                val y = random.nextDouble();//0-1
                val count = random.nextInt(5);//0-5
                val request = new CreateRequest.Builder<Point>()
                        .index(index)
                        .id(String.valueOf(i))
                        .document(Point.builder().x(x).y(y).count(count).build())
                        .build();
                try {
                    client.create(request);
                    log.info("i={}", i);
                } catch (IOException e) {
                    if (e instanceof ResponseException) {

                        //check exist
                        if (((ResponseException) e).getResponse().getStatusLine().getStatusCode() == 409) {
                            val r = new UpdateRequest.Builder<Point, Point>()
                                    .index(index)
                                    .id(String.valueOf(i))
                                    //                               .upsert()
                                    .doc(Point.builder().x(x).y(y).count(count).build())
                                    .build();
                            try {
                                client.update(r, Point.class);
                                log.info("update i ={}", i);
                            } catch (IOException ex) {
                                log.error("update error", ex);
                            }

                        }
                    }
                    log.error("create error ", e);
                }
            } );
        }
    }

    /**
     * 测试搜索 40ms
     *
     * GET map10000/_search
     * {
     *   "aggs": {
     *     "2": {
     *       "range": {
     *         "field": "x",
     *         "ranges": [
     *           {
     *             "to": 0.1,
     *             "from": 0
     *           },
     *           {
     *             "to": 0.2,
     *             "from": 0.1
     *           },
     *           {
     *             "to": 0.3,
     *             "from": 0.2
     *           },
     *           {
     *             "to": 0.4,
     *             "from": 0.3
     *           }
     *         ],
     *         "keyed": true
     *       },
     *       "aggs": {
     *         "3": {
     *           "range": {
     *             "field": "y",
     *             "ranges": [
     *               {
     *                 "to": 0.1,
     *                 "from": 0
     *               },
     *               {
     *                 "to": 0.2,
     *                 "from": 0.1
     *               },
     *               {
     *                 "to": 0.3,
     *                 "from": 0.2
     *               },
     *               {
     *                 "to": 0.4,
     *                 "from": 0.3
     *               }
     *             ],
     *             "keyed": true
     *           }
     *         }
     *       }
     *     }
     *   },
     *   "size": 0,
     *   "fields": [],
     *   "script_fields": {},
     *   "stored_fields": [
     *     "*"
     *   ],
     *   "runtime_mappings": {},
     *   "_source": {
     *     "excludes": []
     *   },
     *   "query": {
     *     "bool": {
     *       "must": [],
     *       "filter": [],
     *       "should": [],
     *       "must_not": []
     *     }
     *   }
     * }
     */
}