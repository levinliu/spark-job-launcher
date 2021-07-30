package org.levintech.sparkjobtrigger.component;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author levin
 * Created on 2021/7/29
 */
@Component
public class FileLocalCopier {
    public void save(InputStream inputStream, String path) {
        try (InputStream stream = inputStream) {
            Files.copy(stream, Paths.get(path));
        } catch (IOException ex) {
            throw new RuntimeException("Fail to save input steam", ex);
        }
    }
}
