package paxos;

import io.grpc.stub.StreamObserver;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;

public class MultiPaxosGRPC extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  private final MultiPaxos multiPaxos;

  public MultiPaxosGRPC(MultiPaxos multiPaxos) {
    this.multiPaxos = multiPaxos;
  }

  @Override
  public void heartbeat(HeartbeatRequest heartbeatRequest,
      StreamObserver<HeartbeatResponse> responseObserver) {
    responseObserver.onNext(multiPaxos.heartbeatHandler(heartbeatRequest));
    responseObserver.onCompleted();
  }


}