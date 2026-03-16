package org.elastic.duplicates.controller;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.elastic.duplicates.DeleteDuplicates;
import org.elastic.duplicates.FindDuplicates;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/duplicates")
@Produces(MediaType.APPLICATION_JSON)
public class DuplicatesController {

    @Inject
    FindDuplicates findDuplicates;

    @Inject
    DeleteDuplicates deleteDuplicates;

    @GET
    @Path("/find")
    public Uni<Map<String, List<String>>> getDuplicates() {
        return findDuplicates.findDuplicates()
                .map(all -> all.entrySet().stream()
                        .filter(e -> e.getValue().size() > 1)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @DELETE
    @Path("/delete")
    public Uni<DeleteDuplicates.DeleteResult> deleteDuplicates() {
        return deleteDuplicates.deleteDuplicates();
    }
}