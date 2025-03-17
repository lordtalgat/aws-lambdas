package kz.talgat.counter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class S3CSVRowCounter implements RequestHandler<S3Event, Integer> {
    S3Client s3Client = S3Client.builder().build();

    @Override
    public Integer handleRequest(S3Event s3Event, Context context) {

        // Get the bucket and object key from the S3 event
        // You can set up more than one bucket it triggers
        String bucketName = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String fileKey = s3Event.getRecords().get(0).getS3().getObject().getKey();

        if (!fileKey.contains(".csv")) return 0;

        try {
            // Get the S3 object
            var inputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(fileKey).build());

            // Read the CSV file
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());

            int rowCount = Math.toIntExact(csvParser.stream().count());

            // Close resources
            csvParser.close();
            reader.close();
            inputStream.close();

            var putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileKey + "_rows_" + rowCount)
                    .build();

            var obj = s3Client.putObject(putObjectRequest, RequestBody.empty());

            s3Client.close();

            return rowCount;
        } catch (IOException | ArithmeticException e) {
            context.getLogger().log("Error processing CSV file: " + e.getMessage());
            throw new RuntimeException("Error processing CSV file", e);
        }
    }
}
