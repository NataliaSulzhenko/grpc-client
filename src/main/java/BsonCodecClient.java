import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.bson.conversions.Bson;
import static io.grpc.MethodDescriptor.generateFullMethodName;

public class BsonCodecClient {

  private static final String SERVICE_NAME = "BSONCodec.Test";
  private final ManagedChannel channel;
  private static volatile io.grpc.MethodDescriptor<Bson, Bson> getSayHelloMethod;
  private static final Object mutex = new Object();

  public BsonCodecClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build();
  }

  public Bson sayHello(Bson request) {
    return io.grpc.stub.ClientCalls.blockingUnaryCall(channel, getSayHelloMethod(), CallOptions.DEFAULT, request);
  }

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Send",
      requestType = Bson.class,
      responseType = Bson.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Bson, Bson> getSayHelloMethod() {
    io.grpc.MethodDescriptor<Bson, Bson> getSayHelloMethod = null;
    if (getSayHelloMethod == null) {
      synchronized (mutex) {
        if (getSayHelloMethod  == null) {
           getSayHelloMethod =
              io.grpc.MethodDescriptor.<Bson, Bson>newBuilder()
                  .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                  .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Send"))
                  .setSampledToLocalTracing(true)
                  .setRequestMarshaller(BsonMarshaller.bsonMarshaller())
                  .setResponseMarshaller(BsonMarshaller.bsonMarshaller())
//                  .setSchemaDescriptor()
                  .build();
        }
      }
    }
    return getSayHelloMethod;
  }
}
