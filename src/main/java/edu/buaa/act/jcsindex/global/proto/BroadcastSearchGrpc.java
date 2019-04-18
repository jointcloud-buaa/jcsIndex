package edu.buaa.act.jcsindex.global.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.20.0)",
    comments = "Source: broadcast.proto")
public final class BroadcastSearchGrpc {

  private BroadcastSearchGrpc() {}

  public static final String SERVICE_NAME = "proto.BroadcastSearch";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<BroadcastProto.BroadRequest,
      BroadcastProto.BroadResponse> getSearchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "search",
      requestType = BroadcastProto.BroadRequest.class,
      responseType = BroadcastProto.BroadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<BroadcastProto.BroadRequest,
      BroadcastProto.BroadResponse> getSearchMethod() {
    io.grpc.MethodDescriptor<BroadcastProto.BroadRequest, BroadcastProto.BroadResponse> getSearchMethod;
    if ((getSearchMethod = BroadcastSearchGrpc.getSearchMethod) == null) {
      synchronized (BroadcastSearchGrpc.class) {
        if ((getSearchMethod = BroadcastSearchGrpc.getSearchMethod) == null) {
          BroadcastSearchGrpc.getSearchMethod = getSearchMethod =
              io.grpc.MethodDescriptor.<BroadcastProto.BroadRequest, BroadcastProto.BroadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "proto.BroadcastSearch", "search"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  BroadcastProto.BroadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  BroadcastProto.BroadResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BroadcastSearchMethodDescriptorSupplier("search"))
                  .build();
          }
        }
     }
     return getSearchMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BroadcastSearchStub newStub(io.grpc.Channel channel) {
    return new BroadcastSearchStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BroadcastSearchBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BroadcastSearchBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BroadcastSearchFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BroadcastSearchFutureStub(channel);
  }

  /**
   */
  public static abstract class BroadcastSearchImplBase implements io.grpc.BindableService {

    /**
     */
    public void search(BroadcastProto.BroadRequest request,
        io.grpc.stub.StreamObserver<BroadcastProto.BroadResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSearchMethod(), responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSearchMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                BroadcastProto.BroadRequest,
                BroadcastProto.BroadResponse>(
                  this, METHODID_SEARCH)))
          .build();
    }
  }

  /**
   */
  public static final class BroadcastSearchStub extends io.grpc.stub.AbstractStub<BroadcastSearchStub> {
    private BroadcastSearchStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BroadcastSearchStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected BroadcastSearchStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BroadcastSearchStub(channel, callOptions);
    }

    /**
     */
    public void search(BroadcastProto.BroadRequest request,
        io.grpc.stub.StreamObserver<BroadcastProto.BroadResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSearchMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class BroadcastSearchBlockingStub extends io.grpc.stub.AbstractStub<BroadcastSearchBlockingStub> {
    private BroadcastSearchBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BroadcastSearchBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected BroadcastSearchBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BroadcastSearchBlockingStub(channel, callOptions);
    }

    /**
     */
    public BroadcastProto.BroadResponse search(BroadcastProto.BroadRequest request) {
      return blockingUnaryCall(
          getChannel(), getSearchMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BroadcastSearchFutureStub extends io.grpc.stub.AbstractStub<BroadcastSearchFutureStub> {
    private BroadcastSearchFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BroadcastSearchFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected BroadcastSearchFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BroadcastSearchFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<BroadcastProto.BroadResponse> search(
        BroadcastProto.BroadRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSearchMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEARCH = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BroadcastSearchImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BroadcastSearchImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEARCH:
          serviceImpl.search((BroadcastProto.BroadRequest) request,
              (io.grpc.stub.StreamObserver<BroadcastProto.BroadResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BroadcastSearchBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BroadcastSearchBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return BroadcastProto.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BroadcastSearch");
    }
  }

  private static final class BroadcastSearchFileDescriptorSupplier
      extends BroadcastSearchBaseDescriptorSupplier {
    BroadcastSearchFileDescriptorSupplier() {}
  }

  private static final class BroadcastSearchMethodDescriptorSupplier
      extends BroadcastSearchBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BroadcastSearchMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (BroadcastSearchGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BroadcastSearchFileDescriptorSupplier())
              .addMethod(getSearchMethod())
              .build();
        }
      }
    }
    return result;
  }
}
