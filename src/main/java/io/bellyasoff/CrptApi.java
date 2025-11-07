package io.bellyasoff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author IlyaB
 */
public class CrptApi {

   public static void main( String[] args ) throws IOException, InterruptedException {
      CrptApi api = new CrptApi(TimeUnit.MINUTES, 10, "https://ismp.crpt.ru/api/v3");

      Map<String, Object> doc = Map.of(
              "inn", "1234567890",
              "productCode", "01234567890123",
              "quantity", 100,
              "timestamp", Instant.now().toString()
      );

      String signature = "BASE64_SIGNATURE_STRING";

      HttpResponse<String> response = api.createDocument(doc, signature);
      System.out.println("Status: " + response.statusCode());
      System.out.println("Body: " + response.body());
   }

   private static final String DEFAULT_API_URL = "https://ismp.crpt.ru/api/v3";

   private final HttpClient httpClient;
   private final ObjectMapper objectMapper;
   private final RateLimiter rateLimiter;
   private final String baseUrl;


   public CrptApi(TimeUnit timeUnit, int requestLimit) {
      this(timeUnit, requestLimit, DEFAULT_API_URL);
   }

   public CrptApi(TimeUnit timeUnit, int requestLimit, String baseUrl) {
      Objects.requireNonNull(timeUnit, "timeUnit cannot be null");
      if (requestLimit <= 0) {
         throw new IllegalArgumentException("requestLimit must be greater than 0");
      }
      this.httpClient = HttpClient.newHttpClient();
      this.objectMapper = new ObjectMapper();
      this.rateLimiter = new RateLimiter(timeUnit.toMillis(1), requestLimit);
      this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl");
   }

   public HttpResponse<String> createDocument(Object document, String signature)
           throws IOException, InterruptedException {
      Objects.requireNonNull(document, "document cannot be null");
      Objects.requireNonNull(signature, "signature cannot be null");

      rateLimiter.acquire();

      String body = serializePayload(document, signature);

      HttpRequest request = HttpRequest.newBuilder()
              .uri(URI.create(baseUrl))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
   }

   private String serializePayload(Object document, String signature) throws JsonProcessingException {
      Map<String, Object> payload = Map.of(
              "document", document,
              "signature", signature
      );
      return objectMapper.writeValueAsString(payload);
   }

   public static final class RateLimiter {
      private final long windowMillis;
      private final int limit;
      private final Deque<Long> timestamps = new ArrayDeque<>();

      RateLimiter(final long windowMillis, final int limit) {
         if (windowMillis <= 0) throw new IllegalArgumentException("Window time must be positive");
         if (limit <= 0) throw new IllegalArgumentException("Limit must be positive");
         this.windowMillis = windowMillis;
         this.limit = limit;
      }

      public void acquire() throws InterruptedException {
         synchronized (this) {
            while (true) {
               long now = System.currentTimeMillis();
               cleanOld(now);

               if (timestamps.size() < limit) {
                  timestamps.add(now);
                  return;
               } else {
                  long oldest = timestamps.peekFirst();
                  long waitFor = windowMillis - (now - oldest);
                  if (waitFor <= 0) {
                     cleanOld(now);
                     continue;
                  }

                  this.wait(waitFor);
               }
            }
         }
      }

      private void cleanOld(long now) {
         while (!timestamps.isEmpty()) {
            long diff = now - timestamps.peekFirst();
            if (diff >= windowMillis) {
               timestamps.removeFirst();
            } else {
               break;
            }
         }
      }
   }

}
