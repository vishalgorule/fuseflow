package io.fuseflow.engine.ha;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineShardsTest {

    @Test
    void shardOfIsDeterministicAndInRange() {
        EngineShards shards = new EngineShards(8, "all");
        UUID id = UUID.randomUUID();
        assertThat(shards.shardOf(id)).isEqualTo(shards.shardOf(id)).isBetween(0, 7);
    }

    @Test
    void shardOfIsIdenticalAcrossInstances() {
        // The whole point of HA: the mapping must not depend on which instance computes it.
        EngineShards a = new EngineShards(8, "all");
        EngineShards b = new EngineShards(8, "all");
        UUID id = UUID.randomUUID();
        assertThat(a.shardOf(id)).isEqualTo(b.shardOf(id));
    }

    @Test
    void ownsAllCoversEveryShard() {
        EngineShards shards = new EngineShards(8, "all");
        assertThat(shards.ownsAll()).isTrue();
        for (int i = 0; i < 8; i++) {
            assertThat(shards.owns(i)).isTrue();
        }
    }

    @Test
    void parsesRange() {
        EngineShards shards = new EngineShards(8, "2-4");
        assertThat(shards.ownedShards()).containsExactly(2, 3, 4);
        assertThat(shards.owns(2)).isTrue();
        assertThat(shards.owns(1)).isFalse();
        assertThat(shards.ownsAll()).isFalse();
    }

    @Test
    void parsesListAndMixedSpec() {
        EngineShards shards = new EngineShards(8, "0,1,6-7");
        assertThat(shards.ownedShards()).containsExactly(0, 1, 6, 7);
    }

    @Test
    void rejectsOutOfRangeShard() {
        assertThatThrownBy(() -> new EngineShards(4, "0-4"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsZeroShardCount() {
        assertThatThrownBy(() -> new EngineShards(0, "all"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
