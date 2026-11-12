package ru.ppp.lab4.common;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Корень проекта, от которого разрешаются исходники и витрина (не зависит от cwd). */
public final class ProjectPaths {

    private ProjectPaths() {
    }

    public static Path projectRoot() {
        try {
            Path location = Paths.get(ProjectPaths.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI()).toAbsolutePath().normalize();
            if (Files.isRegularFile(location)) {
                location = location.getParent();
            }
            while (location != null
                    && ("target".equals(String.valueOf(location.getFileName()))
                    || "classes".equals(String.valueOf(location.getFileName())))) {
                location = location.getParent();
            }
            if (location != null && Files.exists(location.resolve("players.csv"))) {
                return location;
            }
        } catch (URISyntaxException | RuntimeException ignored) {
            // fallback на cwd
        }
        return Paths.get("").toAbsolutePath().normalize();
    }
}
