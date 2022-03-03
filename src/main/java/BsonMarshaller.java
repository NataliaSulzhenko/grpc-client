import com.google.protobuf.Message;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;

public class BsonMarshaller {

  private BsonMarshaller() {}

  public static <T extends Bson> Marshaller<T> bsonMarshaller() {
    return createBsonMarshaller();
  }

  public static <T extends Bson> Marshaller<T> createBsonMarshaller() {

    final Codec<Document> DOCUMENT_CODEC = new DocumentCodec();

    return new Marshaller<T>() {
      @Override
      public InputStream stream(T value) {
        try {
          BasicOutputBuffer buffer = new BasicOutputBuffer();
          BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
          DOCUMENT_CODEC.encode(
              writer,
              (Document) value,
              EncoderContext.builder().isEncodingCollectibleDocument(true).build());
          byte[] bytes = buffer.toByteArray();
          buffer.close();
          return new ByteArrayInputStream(bytes);
        } catch (Exception ex) {
          throw Status.INTERNAL
              .withCause(ex)
              .withDescription("Unable to print bson document")
              .asRuntimeException();
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public T parse(InputStream stream) {
        try {
          BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(stream.readAllBytes()));
          T document = (T) DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
          reader.close();
          return document;
        } catch (IOException e) {
          throw Status.INTERNAL.withDescription("Invalid bson byte sequence")
              .withCause(e).asRuntimeException();
        } catch (Exception ex) {
          throw Status.INTERNAL.withDescription("Unexpected error occurred")
              .withCause(ex).asRuntimeException();
        }
      }
    };
  }
}
