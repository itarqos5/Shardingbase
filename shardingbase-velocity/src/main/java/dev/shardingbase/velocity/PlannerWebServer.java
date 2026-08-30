package dev.shardingbase.velocity;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/** Token-authenticated, single-use world cut planner served from Velocity's dedicated web allocation. */
final class PlannerWebServer implements AutoCloseable {
    private static final int MAX_FORM_BYTES = 16_384;
    private static final String COOKIE_NAME = "ShardingbasePlanner";

    private final WorldPlannerStore store;
    private final BackendRegistry registry;
    private final Logger logger;
    private final HttpServer server;
    private final ThreadPoolExecutor clients;
    private final boolean secureCookie;
    private final java.util.function.Consumer<UUID> transactionStarter;

    PlannerWebServer(
        final VelocityConfiguration configuration,
        final WorldPlannerStore store,
        final BackendRegistry registry,
        final java.util.function.Consumer<UUID> transactionStarter,
        final Logger logger
    ) throws IOException {
        this.store = store;
        this.registry = registry;
        this.transactionStarter = transactionStarter;
        this.logger = logger;
        this.secureCookie = configuration.webPublicUrl().startsWith("https://");
        this.server = HttpServer.create(new InetSocketAddress(
            InetAddress.getByName(configuration.webBindAddress()), configuration.webPort()
        ), 64);
        this.clients = new ThreadPoolExecutor(
            2,
            8,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(128),
            task -> Thread.ofPlatform().daemon(true).name("Shardingbase Planner HTTP").unstarted(task),
            new ThreadPoolExecutor.AbortPolicy()
        );
        this.server.setExecutor(this.clients);
        this.server.createContext("/planner/", this::handle);
        this.server.start();
    }

    private void handle(final HttpExchange exchange) throws IOException {
        try {
            securityHeaders(exchange);
            final String[] segments = exchange.getRequestURI().getPath().substring(1).split("/");
            if (segments.length == 2 && "planner".equals(segments[0]) && "GET".equals(exchange.getRequestMethod())) {
                this.redeem(exchange, segments[1]);
                return;
            }
            if (segments.length >= 3 && "planner".equals(segments[0]) && "session".equals(segments[1])) {
                final UUID sessionId = uuid(segments[2]);
                final WorldPlannerStore.Session session = this.authenticate(exchange, sessionId).orElse(null);
                if (session == null) {
                    send(exchange, 403, "text/plain; charset=utf-8", "Planner session is not authorized.");
                    return;
                }
                if (segments.length == 3 && "GET".equals(exchange.getRequestMethod())) {
                    this.page(exchange, session);
                    return;
                }
                if (segments.length == 7 && "tile".equals(segments[3]) && "GET".equals(exchange.getRequestMethod())) {
                    this.tile(exchange, sessionId, integer(segments[4]), integer(segments[5]));
                    return;
                }
                if (segments.length == 4 && "confirm".equals(segments[3])
                    && "POST".equals(exchange.getRequestMethod())) {
                    this.confirm(exchange, session);
                    return;
                }
            }
            send(exchange, 404, "text/plain; charset=utf-8", "Not found.");
        } catch (final IllegalArgumentException exception) {
            send(exchange, 400, "text/plain; charset=utf-8", "Invalid planner request.");
        } catch (final IOException exception) {
            this.logger.warn("Shardingbase planner request failed", exception);
            send(exchange, 500, "text/plain; charset=utf-8", "Planner request failed safely.");
        } finally {
            exchange.close();
        }
    }

    private void redeem(final HttpExchange exchange, final String linkToken) throws IOException {
        final Optional<WorldPlannerStore.Redeemed> redeemed = this.store.redeem(linkToken);
        if (redeemed.isEmpty()) {
            send(exchange, 410, "text/plain; charset=utf-8", "This one-use planner link is invalid or already used.");
            return;
        }
        final WorldPlannerStore.Redeemed value = redeemed.orElseThrow();
        exchange.getResponseHeaders().add(
            "Set-Cookie",
            COOKIE_NAME + '=' + value.session().sessionId() + '.' + value.browserToken()
                + "; Path=/planner/; HttpOnly; SameSite=Strict" + (this.secureCookie ? "; Secure" : "")
        );
        exchange.getResponseHeaders().add(
            "Location", "/planner/session/" + value.session().sessionId()
        );
        exchange.sendResponseHeaders(303, -1);
    }

    private Optional<WorldPlannerStore.Session> authenticate(final HttpExchange exchange, final UUID sessionId)
        throws IOException {
        final String cookies = exchange.getRequestHeaders().getFirst("Cookie");
        if (cookies == null) {
            return Optional.empty();
        }
        for (final String cookie : cookies.split(";")) {
            final String candidate = cookie.strip();
            if (!candidate.startsWith(COOKIE_NAME + '=')) {
                continue;
            }
            final String value = candidate.substring(COOKIE_NAME.length() + 1);
            final int separator = value.indexOf('.');
            if (separator < 1 || !sessionId.toString().equals(value.substring(0, separator))) {
                return Optional.empty();
            }
            return this.store.authenticate(sessionId, value.substring(separator + 1));
        }
        return Optional.empty();
    }

    private void page(final HttpExchange exchange, final WorldPlannerStore.Session session) throws IOException {
        final List<WorldPlannerStore.TileCoordinate> tiles = this.store.tiles(session.sessionId());
        if (tiles.isEmpty()) {
            send(exchange, 409, "text/plain; charset=utf-8", "The map session contains no rendered tiles.");
            return;
        }
        final int minTileX = tiles.stream().mapToInt(WorldPlannerStore.TileCoordinate::x).min().orElseThrow();
        final int minTileZ = tiles.stream().mapToInt(WorldPlannerStore.TileCoordinate::z).min().orElseThrow();
        final int maxTileX = tiles.stream().mapToInt(WorldPlannerStore.TileCoordinate::x).max().orElseThrow();
        final int maxTileZ = tiles.stream().mapToInt(WorldPlannerStore.TileCoordinate::z).max().orElseThrow();
        final StringBuilder images = new StringBuilder();
        for (final WorldPlannerStore.TileCoordinate tile : tiles) {
            images.append("<img alt=\"map tile\" draggable=\"false\" src=\"/planner/session/")
                .append(session.sessionId()).append("/tile/").append(tile.x()).append('/').append(tile.z())
                .append("/tile.png\" style=\"left:").append((tile.x() - minTileX) * 256)
                .append("px;top:").append((tile.z() - minTileZ) * 256).append("px\">");
        }
        final List<BackendRegistry.BackendTarget> backends = this.registry.backends();
        if (backends.size() != 2) {
            send(exchange, 409, "text/plain; charset=utf-8", "Exactly two validated backends are required.");
            return;
        }
        final String options = backends.stream().map(backend -> "<option value=\"" + html(backend.serverId()) + "\">"
            + html(backend.serverName()) + "</option>").reduce("", String::concat);
        final String page = """
            <!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
            <title>Shardingbase World Planner</title><style>
            :root{color-scheme:dark;font:15px system-ui;background:#111;color:#eee}body{margin:0;display:grid;grid-template-columns:340px 1fr;height:100vh}
            aside{padding:24px;border-right:1px solid #333;overflow:auto}main{overflow:auto;background:#080808}.map{position:relative}.map img{position:absolute;width:256px;height:256px;image-rendering:pixelated}
            label{display:block;margin:18px 0 6px}select,input,button{width:100%;box-sizing:border-box;padding:10px;background:#222;color:#fff;border:1px solid #555}
            button{margin-top:24px;background:#8b1e2d;border-color:#d75465;font-weight:700}small{color:#aaa}.stat{padding:9px 0;border-bottom:1px solid #292929}
            </style></head><body><aside><h1>World cut</h1><p><strong>%s</strong></p>
            <div class=stat>Generated chunks: %,d</div><div class=stat>Estimated transfer: %,.1f MiB</div>
            <form method=post action="/planner/session/%s/confirm" onsubmit="return confirm('Create this immutable transaction plan?')">
            <label>Cut axis</label><select name=axis id=axis><option>X</option><option>Z</option></select>
            <label>Cut chunk: <output id=cutOut></output> (block <output id=blockOut></output>)</label><input name=cut id=cut type=range>
            <label>Negative/red half</label><select name=negative>%s</select><label>Positive/blue half</label><select name=positive>%s</select>
            <button type=submit>Confirm immutable plan</button></form><p><small>Ungenerated territory inherits the selected half-plane rule.</small></p></aside>
            <main><div class=map style="width:%dpx;height:%dpx">%s</div></main><script>
            const a=document.querySelector('#axis'),c=document.querySelector('#cut'),o=document.querySelector('#cutOut'),b=document.querySelector('#blockOut');
            const bounds={X:[%d,%d],Z:[%d,%d]};function sync(){const v=bounds[a.value];c.min=v[0]+1;c.max=v[1];if(+c.value<c.min||+c.value>c.max)c.value=Math.floor((v[0]+v[1]+1)/2);o.value=c.value;b.value=c.value*16}a.onchange=sync;c.oninput=sync;sync();
            </script></body></html>
            """.formatted(
                html(session.worldKey()), session.generatedChunks(), session.estimatedBytes() / 1_048_576.0D,
                session.sessionId(), options, options, (maxTileX - minTileX + 1) * 256,
                (maxTileZ - minTileZ + 1) * 256, images, session.minChunkX(), session.maxChunkX(),
                session.minChunkZ(), session.maxChunkZ()
            );
        send(exchange, 200, "text/html; charset=utf-8", page);
    }

    private void tile(final HttpExchange exchange, final UUID sessionId, final int tileX, final int tileZ)
        throws IOException {
        final Optional<byte[]> png = this.store.tile(sessionId, tileX, tileZ);
        if (png.isEmpty()) {
            send(exchange, 404, "text/plain; charset=utf-8", "Tile not found.");
            return;
        }
        send(exchange, 200, "image/png", png.orElseThrow());
    }

    private void confirm(final HttpExchange exchange, final WorldPlannerStore.Session session) throws IOException {
        final int declaredLength = Optional.ofNullable(exchange.getRequestHeaders().getFirst("Content-Length"))
            .map(Integer::parseInt).orElse(0);
        if (declaredLength < 1 || declaredLength > MAX_FORM_BYTES) {
            send(exchange, 413, "text/plain; charset=utf-8", "Invalid confirmation payload.");
            return;
        }
        final byte[] body = exchange.getRequestBody().readNBytes(MAX_FORM_BYTES + 1);
        if (body.length > MAX_FORM_BYTES) {
            send(exchange, 413, "text/plain; charset=utf-8", "Confirmation payload is too large.");
            return;
        }
        final Map<String, String> form = form(new String(body, StandardCharsets.UTF_8));
        final Set<String> backendIds = this.registry.backends().stream()
            .map(BackendRegistry.BackendTarget::serverId).collect(java.util.stream.Collectors.toUnmodifiableSet());
        final String negative = required(form, "negative");
        final String positive = required(form, "positive");
        if (!backendIds.contains(negative) || !backendIds.contains(positive)) {
            throw new IOException("Planner backend assignment is not registered");
        }
        final UUID transactionId = this.store.confirm(
            session,
            required(form, "axis"),
            integer(required(form, "cut")),
            negative,
            positive
        );
        this.transactionStarter.accept(transactionId);
        send(exchange, 201, "text/html; charset=utf-8", "<h1>Plan created</h1><p>Transaction <code>"
            + transactionId + "</code> is immutable and preflight has started.</p>");
    }

    private static Map<String, String> form(final String body) {
        final Map<String, String> values = new HashMap<>();
        for (final String entry : body.split("&")) {
            final int separator = entry.indexOf('=');
            if (separator > 0) {
                values.put(
                    URLDecoder.decode(entry.substring(0, separator), StandardCharsets.UTF_8),
                    URLDecoder.decode(entry.substring(separator + 1), StandardCharsets.UTF_8)
                );
            }
        }
        return values;
    }

    private static String required(final Map<String, String> values, final String field) {
        final String value = values.get(field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing " + field);
        }
        return value;
    }

    private static UUID uuid(final String value) {
        return UUID.fromString(value);
    }

    private static int integer(final String value) {
        return Integer.parseInt(value);
    }

    private static String html(final String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace("\"", "&quot;").replace("'", "&#39;");
    }

    private static void securityHeaders(final HttpExchange exchange) {
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.getResponseHeaders().set("X-Frame-Options", "DENY");
        exchange.getResponseHeaders().set(
            "Content-Security-Policy", "default-src 'self'; img-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'"
        );
        exchange.getResponseHeaders().set("Referrer-Policy", "no-referrer");
    }

    private static void send(
        final HttpExchange exchange,
        final int status,
        final String contentType,
        final String value
    ) throws IOException {
        send(exchange, status, contentType, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void send(
        final HttpExchange exchange,
        final int status,
        final String contentType,
        final byte[] value
    ) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, value.length);
        exchange.getResponseBody().write(value);
    }

    @Override
    public void close() {
        this.server.stop(1);
        this.clients.shutdownNow();
    }
}
