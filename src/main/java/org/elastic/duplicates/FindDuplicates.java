package org.elastic.duplicates;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.elasticsearch.client.RestClient;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class FindDuplicates {

    private static final Logger log = Logger.getLogger(FindDuplicates.class);

    String duplicateCompareFirstField="name";
    String duplicateCompareSecondField="url";

    @ConfigProperty(name = "elastic.username")
    String elasticUsername;

    @ConfigProperty(name = "elastic.password")
    String elasticPassword;

    @ConfigProperty(name = "elastic.index")
    String index;

    @ConfigProperty(name = "elastic.host", defaultValue = "192.168.1.111")
    String elasticHost;

    @ConfigProperty(name = "elastic.port", defaultValue = "9200")
    int elasticPort;

    private RestClient restClient;
    private ElasticsearchClient client;
    private ExecutorService executor;

    @PostConstruct
    void init() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(elasticUsername, elasticPassword));

        restClient = RestClient.builder(new HttpHost(elasticHost, elasticPort, "http"))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .build();

        client = new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper()));

        executor = Executors.newFixedThreadPool(10);

        log.info("ElasticsearchClient initialized");
    }

    @PreDestroy
    void cleanup() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (IOException e) {
                log.error("Failed to close RestClient", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    public ElasticsearchClient getClient() {
        return client;
    }

    public String getIndex() {
        return index;
    }

    @SuppressWarnings("unchecked")
    public Uni<Map<String, List<String>>> findDuplicates() {
        return Uni.createFrom().completionStage(() -> CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Starting duplicate search...");
                Map<String, List<String>> nameToIds = new HashMap<>();

                SearchResponse<Map<String, Object>> searchResponse = client.search(s -> s
                                .index(index)
                                .scroll(t -> t.time("5m"))
                                .size(10000)
                                .source(src -> src.filter(f -> f.includes(duplicateCompareFirstField,
                                        duplicateCompareSecondField))),
                        (Class<Map<String, Object>>) (Class<?>) Map.class);

                int totalProcessed = 0;
                String scrollId = searchResponse.scrollId();
                List<Hit<Map<String, Object>>> hits = searchResponse.hits().hits();

                while (!hits.isEmpty()) {
                    for (Hit<Map<String, Object>> hit : hits) {
                        Map<String, Object> source = hit.source();
                        if (source == null) continue;
                        String name = Objects.toString(source.get(duplicateCompareFirstField), "");
                        String url = Objects.toString(source.get(duplicateCompareSecondField), "");
                        String key = name + "|" + url;
                        nameToIds.computeIfAbsent(key, _ -> new ArrayList<>()).add(hit.id());
                        totalProcessed++;
                    }
                    log.infof("Processed: %d", totalProcessed);

                    String currentScrollId = scrollId;
                    ScrollResponse<Map<String, Object>> scrollResponse = client.scroll(sr -> sr
                                    .scrollId(currentScrollId)
                                    .scroll(t -> t.time("5m")),
                            (Class<Map<String, Object>>) (Class<?>) Map.class);

                    scrollId = scrollResponse.scrollId();
                    hits = scrollResponse.hits().hits();
                }

                String finalScrollId = scrollId;
                client.clearScroll(c -> c.scrollId(finalScrollId));

                long duplicateCount = nameToIds.values().stream().filter(v -> v.size() > 1).count();
                log.infof("Total documents: %d", totalProcessed);
                log.infof("Total duplicate groups: %d", duplicateCount);

                return nameToIds;
            } catch (Exception e) {
                log.error("Failed to find duplicates", e);
                throw new RuntimeException(e);
            }
        }, executor));
    }
}