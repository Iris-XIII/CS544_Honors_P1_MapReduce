"""Exercise the public Boss API over real gRPC connections on ephemeral ports."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
import tempfile
import time
import unittest

import grpc

import boss_pb2
import boss_pb2_grpc
from boss.jobs import JobStore
from boss.server import create_server


class TestBossRPC(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.input_path = self.root / "input.txt"
        self.input_path.write_text("hello world\n", encoding="utf-8")
        self.job_file = self.root / "wordcount.py"
        # Registration must never execute the supplied code.
        self.job_file.write_text(
            "raise RuntimeError('Do not execute code during submission')\n",
            encoding="utf-8",
        )
        self.store = JobStore()
        self.server, port = create_server("127.0.0.1:0", self.store)
        self.server.start()
        self.addCleanup(lambda: self.server.stop(0).wait())
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.addCleanup(self.channel.close)
        grpc.channel_ready_future(self.channel).result(timeout=5)
        self.stub = boss_pb2_grpc.BossServiceStub(self.channel)

    def request(self, **overrides):
        fields = dict(
            input_path=str(self.input_path),
            output_path=str(self.root / "new" / "output"),
            job_file=str(self.job_file),
            num_maps=4,
            num_reducers=2,
        )
        fields.update(overrides)
        return boss_pb2.SubmitJobRequest(**fields)

    def status(self, job_id):
        return self.stub.GetJobStatus(
            boss_pb2.JobStatusRequest(job_id=job_id), timeout=5
        )

    def assert_rpc_error(self, code, operation):
        with self.assertRaises(grpc.RpcError) as caught:
            operation()
        self.assertEqual(caught.exception.code(), code)
        self.assertTrue(caught.exception.details())

    def test_submit_and_query_preserves_job_settings(self):
        before = time.time()
        reply = self.stub.SubmitJob(self.request(), timeout=5)
        self.assertTrue(reply.job_id)
        self.assertEqual(reply.state, boss_pb2.JOB_QUEUED)

        status = self.status(reply.job_id)
        self.assertEqual(status.job_id, reply.job_id)
        self.assertEqual(status.state, boss_pb2.JOB_QUEUED)
        self.assertEqual((status.num_maps, status.num_reducers), (4, 2))
        self.assertEqual((status.completed_maps, status.completed_reducers), (0, 0))
        self.assertEqual(status.output_path, self.request().output_path)
        self.assertEqual(status.error, "")
        self.assertLessEqual(before, status.submitted_at_unix_seconds)
        self.assertLessEqual(status.submitted_at_unix_seconds, time.time())
        record = self.store.get(reply.job_id)
        self.assertEqual(record.spec.input_path, str(self.input_path))
        self.assertEqual(record.spec.job_file, str(self.job_file))
        # Submission and repeated polling must not create output or start work.
        self.assertFalse((self.root / "new").exists())
        self.assertEqual(self.status(reply.job_id), status)

    def test_invalid_counts_are_rejected(self):
        for field in ("num_maps", "num_reducers"):
            for value in (0, -1):
                with self.subTest(field=field, value=value):
                    self.assert_rpc_error(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        lambda: self.stub.SubmitJob(
                            self.request(**{field: value}), timeout=5
                        ),
                    )

    def test_invalid_paths_are_rejected(self):
        for field in ("input_path", "output_path", "job_file"):
            for value in ("", "   ", "relative/path", "/invalid\x00path"):
                with self.subTest(field=field, value=value):
                    self.assert_rpc_error(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        lambda: self.stub.SubmitJob(
                            self.request(**{field: value}), timeout=5
                        ),
                    )

    def test_input_and_job_must_be_existing_files(self):
        for field in ("input_path", "job_file"):
            for value in (str(self.root / "missing"), str(self.root)):
                with self.subTest(field=field, value=value):
                    self.assert_rpc_error(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        lambda: self.stub.SubmitJob(
                            self.request(**{field: value}), timeout=5
                        ),
                    )

    def test_output_cannot_be_a_file_or_have_a_file_parent(self):
        for value in (str(self.input_path), str(self.input_path / "output")):
            with self.subTest(path=value):
                self.assert_rpc_error(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    lambda: self.stub.SubmitJob(
                        self.request(output_path=value), timeout=5
                    ),
                )

    def test_output_cannot_use_a_dangling_symlink(self):
        link = self.root / "dangling"
        link.symlink_to(self.root / "missing")
        self.assert_rpc_error(
            grpc.StatusCode.INVALID_ARGUMENT,
            lambda: self.stub.SubmitJob(
                self.request(output_path=str(link / "output")), timeout=5
            ),
        )

    def test_empty_input_and_existing_output_directory_are_accepted(self):
        self.input_path.write_text("", encoding="utf-8")
        output = self.root / "output"
        output.mkdir()
        reply = self.stub.SubmitJob(
            self.request(output_path=str(output)), timeout=5
        )
        self.assertEqual(self.status(reply.job_id).state, boss_pb2.JOB_QUEUED)
        self.assertEqual(list(output.iterdir()), [])

    def test_empty_and_unknown_job_ids(self):
        for job_id in ("", "   "):
            with self.subTest(job_id=job_id):
                self.assert_rpc_error(
                    grpc.StatusCode.INVALID_ARGUMENT, lambda: self.status(job_id)
                )
        self.assert_rpc_error(
            grpc.StatusCode.NOT_FOUND, lambda: self.status("not-a-real-job")
        )

    def test_repeated_submissions_create_distinct_jobs(self):
        first = self.stub.SubmitJob(self.request(), timeout=5)
        second = self.stub.SubmitJob(self.request(), timeout=5)
        self.assertNotEqual(first.job_id, second.job_id)
        self.assertEqual(self.status(first.job_id).state, boss_pb2.JOB_QUEUED)
        self.assertEqual(self.status(second.job_id).state, boss_pb2.JOB_QUEUED)

    def test_concurrent_submissions_keep_records_separate(self):
        def submit_and_read(index):
            reply = self.stub.SubmitJob(
                self.request(num_maps=index + 1, output_path=str(self.root / str(index))),
                timeout=5,
            )
            return index, self.status(reply.job_id)

        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(submit_and_read, range(40)))
        self.assertEqual(len({status.job_id for _, status in results}), 40)
        for index, status in results:
            self.assertEqual(status.num_maps, index + 1)
            self.assertEqual(status.output_path, str(self.root / str(index)))
            self.assertEqual(status.state, boss_pb2.JOB_QUEUED)

    def test_callers_cannot_mutate_stored_records(self):
        reply = self.stub.SubmitJob(self.request(), timeout=5)
        record = self.store.get(reply.job_id)
        with self.assertRaises(FrozenInstanceError):
            record.state = boss_pb2.JOB_COMPLETED
        with self.assertRaises(FrozenInstanceError):
            record.spec.num_maps = 100
        self.assertEqual(self.status(reply.job_id).num_maps, 4)


if __name__ == "__main__":
    unittest.main()
