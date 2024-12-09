package store.xianglin.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroDemo {
    public static void main(String[] args) throws IOException {
        var user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);

        var user2 = new User("Ben", 7, "red");

        var user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        var file = new File("users.avro");
        // serialize user to disk
        var userDatumWriter = new SpecificDatumWriter<>(User.class);
        try (var dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(user1.getSchema(), file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        }

        // deserialize user from disk
        var userDatumReader = new SpecificDatumReader<User>(User.class);
        try (var dataFileReader = new DataFileReader<>(file, userDatumReader)) {
            User user = new User();
            while (dataFileReader.hasNext()) {
                var next = dataFileReader.next(user);
                System.out.println(next);
            }
        }
    }
}
