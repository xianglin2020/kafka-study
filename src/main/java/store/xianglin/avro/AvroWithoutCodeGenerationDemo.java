package store.xianglin.avro;

import org.apache.avro.SchemaParser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroWithoutCodeGenerationDemo {
    public static void main(String[] args) throws IOException {
        var schema = new SchemaParser().parse(new File("src/main/resources/avro/user.avsc")).mainSchema();

        var user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        var user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        // serialize user to disk
        var file = new File("users.avro");
        var datumWriter = new GenericDatumWriter<>(schema);
        try (var dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
        }

        // deserialize user from disk
        var datumReader = new GenericDatumReader<>(schema);
        try (var dataFileReader = new DataFileReader<>(file, datumReader)) {
            var record = new GenericData.Record(schema);
            while (dataFileReader.hasNext()) {
                dataFileReader.next(record);
                System.out.println(record);
            }
        }

    }
}
