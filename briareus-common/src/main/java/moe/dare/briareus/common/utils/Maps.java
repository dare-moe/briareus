package moe.dare.briareus.common.utils;

import java.util.EnumMap;

public class Maps {
    @SuppressWarnings("squid:S1319")
    public static <K extends Enum<K>, V> EnumMap<K, V> enumMapOf(K k1, V v1) {
        EnumMap<K, V> map = new EnumMap<>(k1.getDeclaringClass());
        map.put(k1, v1);
        return map;
    }

    @SuppressWarnings("squid:S1319")
    public static <K extends Enum<K>, V> EnumMap<K, V> enumMapOf(K k1, V v1, K k2, V v2) {
        EnumMap<K, V> map = new EnumMap<>(k1.getDeclaringClass());
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    @SuppressWarnings("squid:S1319")
    public static <K extends Enum<K>, V> EnumMap<K, V> enumMapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
        EnumMap<K, V> map = new EnumMap<>(k1.getDeclaringClass());
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return map;
    }

    private Maps() {
    }
}
