
import grpc

import mapreduce_pb2
import mapreduce_pb2_grpc


def main():
    # Connect to the locally running Worker server.
    channel = grpc.insecure_channel("localhost:50051")
    grpc.channel_ready_future(channel).result(timeout=5)

    stub = mapreduce_pb2_grpc.WorkerServiceStub(channel)

    # 1. Submit a map task.
    response = stub.RunMap(
        mapreduce_pb2.MapTaskRequest(
            job_id="test_job",
            task_id=0,
            attempt_id=0,
            input_path="/shared/input/test.jsonl",
            job_file="/shared/jobs/wordcount.py",
            num_reducers=2,
            start_line=0,
            end_line=10,
        ),
        timeout=5,
    )

    print("RunMap response:")
    print(response)
    assert response.accepted

    # 2. Check the task status.
    status = stub.GetTaskStatus(
        mapreduce_pb2.TaskStatusRequest(
            job_id="test_job",
            task_id=0,
            attempt_id=0,
            is_reduce=False,
        ),
        timeout=5,
    )

    print("\nTask status:")
    print(status)

    assert status.state == mapreduce_pb2.PENDING

    # 3. Verify duplicate submissions are rejected.
    duplicate = stub.RunMap(
        mapreduce_pb2.MapTaskRequest(
            job_id="test_job",
            task_id=0,
            attempt_id=0,
        ),
        timeout=5,
    )

    assert not duplicate.accepted

    # 4. Submit a reduce task.
    reduce_response = stub.RunReduce(
        mapreduce_pb2.ReduceTaskRequest(
            job_id="test_job",
            task_id=0,
            attempt_id=0,
            intermediate_path="/shared/intermediate",
            output_path="/shared/output",
            job_file="/shared/jobs/wordcount.py",
        ),
        timeout=5,
    )

    assert reduce_response.accepted

    # 5. Check an unknown task.
    unknown = stub.GetTaskStatus(
        mapreduce_pb2.TaskStatusRequest(
            job_id="missing_job",
            task_id=99,
            attempt_id=0,
            is_reduce=False,
        ),
        timeout=5,
    )

    assert unknown.state == mapreduce_pb2.UNKNOWN

    print("\nAll Worker RPC tests passed!")

    channel.close()


if __name__ == "__main__":
    main()
