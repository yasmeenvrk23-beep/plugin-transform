package io.kestra.plugin.transform.arrow;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Transform Arrow, Parquet, CSV, or NDJSON data using jq-style queries via aq."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a query on an Arrow or Parquet file",
            code = {
                "from: \"{{ inputs.file }}\"",
                "query: \".[] | select(.age > 30)\""
            }
        )
    }
)
public class ArrowTransform extends Task implements RunnableTask<ArrowTransform.Output> {

    @Schema(
        title = "Internal storage URI of the file to transform"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The jq-style transformation query to execute using aq"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String query;

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI renderFrom = URI.create(runContext.render(this.from));
        String renderQuery = runContext.render(this.query);

        File inputFile = runContext.workingDir().createTempFile(".data").toFile();
        try (FileOutputStream out = new FileOutputStream(inputFile)) {
            runContext.storage().getFile(renderFrom).transferTo(out);
        }

        File outputFile = runContext.workingDir().createTempFile(".arrow").toFile();

        ProcessBuilder processBuilder = new ProcessBuilder("aq", renderQuery, inputFile.getAbsolutePath());
        processBuilder.redirectOutput(outputFile);
        
        Process process = processBuilder.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("aq execution failed with exit code: " + exitCode);
        }

        URI outputUri;
        try (FileInputStream in = new FileInputStream(outputFile)) {
            outputUri = runContext.storage().put(renderFrom, in);
        }

        return Output.builder()
            .uri(outputUri)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the transformed Arrow output file"
        )
        private final URI uri;
    }
}
