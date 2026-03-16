package org.elastic.duplicates;

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class DeleteDuplicates {

    private static final Logger log = Logger.getLogger(DeleteDuplicates.class);
    private static final int BULK_SIZE = 1000;

    @Inject
    FindDuplicates findDuplicates;

    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public Uni<DeleteResult> deleteDuplicates() {
        return findDuplicates.findDuplicates()
                .chain(nameToIds -> Uni.createFrom().completionStage(() ->
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                AtomicInteger totalDeleted = new AtomicInteger(0);
                                AtomicInteger totalFailed = new AtomicInteger(0);
                                BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
                                int bufferCount = 0;

                                for (Map.Entry<String, List<String>> entry : nameToIds.entrySet()) {
                                    List<String> ids = entry.getValue();
                                    if (ids.size() <= 1) continue;

                                    String[] parts = entry.getKey().split("\\|", 2);
                                    log.infof("Deleting %d duplicates for: name='%s' documentPath='%s'",
                                            ids.size() - 1,
                                            parts[0],
                                            parts.length > 1 ? parts[1] : "");

                                    for (int i = 1; i < ids.size(); i++) {
                                        String id = ids.get(i);
                                        bulkBuilder.operations(op -> op
                                                .delete(d -> d
                                                        .index(findDuplicates.getIndex())
                                                        .id(id)));
                                        bufferCount++;

                                        if (bufferCount >= BULK_SIZE) {
                                            flush(bulkBuilder.build(), totalDeleted, totalFailed);
                                            bulkBuilder = new BulkRequest.Builder();
                                            bufferCount = 0;
                                        }
                                    }
                                }

                                if (bufferCount > 0) {
                                    flush(bulkBuilder.build(), totalDeleted, totalFailed);
                                }

                                log.infof("Delete complete. Deleted: %d, Failed: %d",
                                        totalDeleted.get(), totalFailed.get());

                                return new DeleteResult(totalDeleted.get(), totalFailed.get());
                            } catch (Exception e) {
                                log.error("Failed to delete duplicates", e);
                                throw new RuntimeException(e);
                            }
                        }, executor)));
    }

    private void flush(BulkRequest request, AtomicInteger totalDeleted, AtomicInteger totalFailed) throws Exception {
        BulkResponse response = findDuplicates.getClient().bulk(request);
        if (response.errors()) {
            for (BulkResponseItem item : response.items()) {
                if (item.error() != null) {
                    log.errorf("Failed to delete id=%s: %s", item.id(), item.error().reason());
                    totalFailed.incrementAndGet();
                } else {
                    totalDeleted.incrementAndGet();
                }
            }
        } else {
            totalDeleted.addAndGet(response.items().size());
        }
        log.infof("Bulk delete batch: %d items", response.items().size());
    }

    public record DeleteResult(int deleted, int failed) {}
}