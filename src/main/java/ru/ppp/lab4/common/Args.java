package ru.ppp.lab4.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Ручной разбор CLI-флагов: --flag value и boolean-флаги без значения. */
public final class Args {

    public static final class ArgException extends Exception {
        public ArgException(String message) {
            super(message);
        }
    }

    private final Map<String, String> values;

    private Args(Map<String, String> values) {
        this.values = values;
    }

    public static Args parse(String[] argv, Set<String> valueFlags, Set<String> booleanFlags)
            throws ArgException {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];
            if (!arg.startsWith("--") || arg.length() == 2) {
                throw new ArgException("неизвестный аргумент: " + arg);
            }
            String key = arg.substring(2);
            if (booleanFlags.contains(key)) {
                values.put(key, "true");
            } else if (valueFlags.contains(key)) {
                if (i + 1 >= argv.length || argv[i + 1].startsWith("--")) {
                    throw new ArgException("для --" + key + " не указано значение");
                }
                values.put(key, argv[++i]);
            } else {
                throw new ArgException("неизвестный аргумент: " + arg);
            }
        }
        return new Args(values);
    }

    public String get(String key) {
        return values.get(key);
    }

    public String require(String key) throws ArgException {
        String value = values.get(key);
        if (value == null) {
            throw new ArgException("обязательный аргумент --" + key + " не указан");
        }
        return value;
    }

    public String getOr(String key, String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }

    public boolean has(String key) {
        return values.containsKey(key);
    }
}
