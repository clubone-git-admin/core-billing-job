package io.clubone.billing.security;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

@Component
public class CaffeineActorCache implements ActorCache {

  private final Cache<CacheKey, Object> cache;

  public CaffeineActorCache() {
    this.cache = Caffeine.newBuilder()
        .maximumSize(10_000)
        .expireAfterWrite(60, TimeUnit.SECONDS)
        .recordStats()
        .build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(UUID actorId, UUID workingLocationId, BiFunction<UUID, UUID, T> loader) {
    UUID locKey = workingLocationId != null ? workingLocationId : new UUID(0L, 0L);
    return (T) cache.get(new CacheKey(actorId, locKey),
        k -> loader.apply(k.actorId(), workingLocationId));
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  private record CacheKey(UUID actorId, UUID locationId) {
  }
}
