import org.bson.Document;

public class Main {

  public static void main(String[] args) {
    BsonCodecClient client = new BsonCodecClient("localhost",50051);
    Document document = new Document("hello", "world");
    client.sayHello(document);
  }
}
