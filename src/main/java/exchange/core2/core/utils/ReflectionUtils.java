package exchange.core2.core.utils;

import java.lang.reflect.Field;

public final class ReflectionUtils {

    @SuppressWarnings(value = {"unchecked"})
    public static <R, T> R extractField(Class<T> clazz, T object, String fieldName) {
        try {
            final Field f = getField(clazz, fieldName);
            f.setAccessible(true);
            return (R) f.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }

    public static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                throw e;
            } else {
                return getField(superClass, fieldName);
            }
        }
    }

}
