import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.CoordsGeoBounds;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.GeoBounds;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationRange;
import co.elastic.clients.elasticsearch._types.aggregations.RangeAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.RangeBucket;
import co.elastic.clients.elasticsearch._types.aggregations.SumAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.SumAggregation;
import co.elastic.clients.elasticsearch._types.mapping.FloatNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.GeoPointProperty;
import co.elastic.clients.elasticsearch._types.mapping.IntegerNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoBoundingBoxQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.braindata.api.ApiApplication;
import com.braindata.api.model.GeoPoint;
import com.braindata.api.model.Point;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpHost;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/3/15 9:41 上午
 */

@Slf4j
@SpringBootTest(classes= ApiApplication.class)
public class ElasticSearchGeo {

    //es 客户端
    private ElasticsearchClient client;

    private String index = "map_geo";

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
                        put("location", new Property(new GeoPointProperty.Builder().build()));
                    }}).build()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * geo
     * 7.17.0的范围可以超过180
     * 版本8.1.0 做了范围限制，所以无法超过180
     */
    @Test
    public void insertSingleData() {
        Random random = new Random();
        for (int j = 0; j <1000000;j++) { //maybe bulk can batch insert
            final int i = j;
            executor.submit( () -> {
                val x = random.nextDouble() * 1000;//0-1
                val y = random.nextDouble() * 1000;//0-1
                val count = random.nextInt(5);//0-5
                val request = new CreateRequest.Builder<GeoPoint>()
                        .index(index)
                        .id(String.valueOf(i))
                        .document(GeoPoint.builder()
//                                .x(x)
//                                .y(y)
                                .count(count)
                                .location(org.springframework.data.elasticsearch.core.geo.GeoPoint.fromPoint(new org.springframework.data.geo.Point(x,y)))
                                .build())
                        .build();
                try {
                    val response = client.create(request);
                    log.info("i={}", i);
                } catch (IOException e) {
                    if (e instanceof ResponseException) {

                        //check exist
                        if (((ResponseException) e).getResponse().getStatusLine().getStatusCode() == 409) {
                            val r = new UpdateRequest.Builder<GeoPoint, GeoPoint>()
                                    .index(index)
                                    .id(String.valueOf(i))
                                    //                               .upsert()
                                    .doc(GeoPoint.builder()
//                                .x(x)
//                                .y(y)
                                            .count(count)
                                            .location(org.springframework.data.elasticsearch.core.geo.GeoPoint.fromPoint(new org.springframework.data.geo.Point(x,y)))
                                            .build())
                                    .build();
                            try {
                                client.update(r, GeoPoint.class);
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

    @Test
    public void importData() {
        Random random = new Random();
        for (int j = 40000; j <100000; j++) { //循环10万次,
            final int i = j;
            executor.submit( () -> {
                val list = new ArrayList<Long>();
                for (long k= i *1000; k< (i+1)*1000;k++) {
                    list.add(k);
                }

                val request = new BulkRequest.Builder()
                        .index(index)
                        .operations(list.stream().map(p-> {
                            var x = random.nextDouble();//0-1
                            var y = random.nextDouble();//0-1
                            if (x<0.5) { //往中心偏移
                                x += Math.abs(0.5f -x) / 2;
                            }else {
                                x -= Math.abs(0.5f -x) / 2;
                            }

                            if (y<0.5) { //往中心偏移
                                y += Math.abs(0.5f -y) / 2;
                            }else {
                                y -= Math.abs(0.5f -y) / 2;
                            }
                            val count = random.nextInt(5);//0-5
                            x *= 10000;
                            y *= 10000;
                            log.info("p={} x={}, y={}, count={}",p, x, y, count);
                            return new BulkOperation.Builder()
                                .create(new CreateOperation.Builder<GeoPoint>()
                                        .id(String.valueOf(p))
                                        .document(GeoPoint.builder()
//                                                .x(x)
//                                                .y(y)
                                                .count(count)
                                                .location(org.springframework.data.elasticsearch.core.geo.GeoPoint.fromPoint(
                                                        new org.springframework.data.geo.Point(x, y))
                                                )
                                                .build())
                                        .build())
                                .build();
                        }).collect(Collectors.toList())) //每个bulk 1000个点
                        .build();
                try {
                    client.bulk(request);
                    log.info("i={}", i);
                } catch (IOException e) {
                    log.error("create error ", e);
                }
            } );
        }
    }

    /**
     * 测试搜索
     */
    @Test
    public void search() {

        List<AggregationRange> xRange = new ArrayList<>();
        List<AggregationRange> yRange = new ArrayList<>();
        float max = 1000.f;//精度
        float startPos = 0.1f; //起点
        float endPos = 0.5f; //终点

        float step = (endPos - startPos) / max; //步长

        for (float i = startPos; i<endPos; i+=step) {
            xRange.add(new AggregationRange.Builder()
                            .from(decimal(i))
                            .to(decimal(i + step))
                    .build());
        }
        try {
            val start = System.currentTimeMillis();
            log.info("start search");
            val query = new GeoBoundingBoxQuery.Builder()
                    .field("location")
                    .boundingBox(new GeoBounds.Builder()
                            .coords(new CoordsGeoBounds.Builder()
                                    .top(70) //top是latitude, left/right是 longitude //经度需要在-180，180之间，纬度在0-90之间。且top>bottom. left right 没有限制
                                    .left(0)
                                    .right(10)
                                    .bottom(60)
                                    .build())
                            .build())
                    .build();

            try {
                val response = client.search(new SearchRequest.Builder()
                        .index(index)
                        .query(new Query(query))
                        .build(), Object.class);
                log.info("response={}", response);
            } catch (IOException e) {
                log.error("io error", e);
            }
            val end = System.currentTimeMillis();
            log.info("cost =  {}", end - start);
        } catch (ElasticsearchException e) {
            log.error("error {}",e.response().error().rootCause().stream().map(ErrorCause::reason).collect(Collectors.toList()), e);
        }
    }

    /**
     * 保留小数点后4位
     */
    private String decimal(float x){
        return String.format("%.4f", x);
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