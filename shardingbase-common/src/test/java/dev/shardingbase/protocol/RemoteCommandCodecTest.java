package dev.shardingbase.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RemoteCommandCodecTest {
    @Test
    void roundTripsCatalogRequestsAndCapturedOutput() throws Exception {
        final RemoteCommandCodec.Catalog catalog = new RemoteCommandCodec.Catalog(
            "backend-a", Set.of("home", "plugin:admin")
        );
        assertEquals(catalog, RemoteCommandCodec.decodeCatalog(RemoteCommandCodec.encodeCatalog(catalog)));
        final UUID id = UUID.randomUUID();
        final RemoteCommandCodec.Request request = new RemoteCommandCodec.Request(
            id, "backend-b", RemoteCommandCodec.Operation.EXECUTE, "home spawn"
        );
        assertEquals(request, RemoteCommandCodec.decodeRequest(RemoteCommandCodec.encodeRequest(request)));
        final RemoteCommandCodec.Response response = new RemoteCommandCodec.Response(
            id, RemoteCommandCodec.Outcome.SUCCESS, "executed", List.of("Done")
        );
        assertEquals(response, RemoteCommandCodec.decodeResponse(RemoteCommandCodec.encodeResponse(response)));
    }
}
