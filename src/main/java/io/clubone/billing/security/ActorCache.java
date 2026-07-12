package io.clubone.billing.security;

import java.util.UUID;
import java.util.function.BiFunction;

public interface ActorCache {
  <T> T get(UUID actorId, UUID workingLocationId, BiFunction<UUID, UUID, T> loader);

  void invalidateAll();
}
