"""Scheduler integration tests with controlled Workers over real gRPC.

These Workers simulate task completion and output files; they do not implement
or validate Iris's map/shuffle/reduce algorithms.
"""

from concurrent import futures
import json
from pathlib import Path
import tempfile
from threading import Event, Lock
from time import monotonic
import unittest

import grpc

import boss_pb2
import boss_pb2_grpc
import mapreduce_pb2 as worker_pb2
import mapreduce_pb2_grpc as worker_grpc
from boss.jobs import JobSpec, JobStore
from boss.scheduler import Scheduler, line_ranges
from boss.server import create_server


class ControlledWorker(worker_grpc.WorkerServiceServicer):
    def __init__(self, root, events, events_lock):
        self.root = root
        self.events = events
        self.events_lock = events_lock
        self.lock = Lock()
        self.gate = Event()
        self.gate.set()
        self.current = None
        self.calls = []
        self.polls = 0
        self.overlaps = 0
        self.reject = False
        self.fail_reduce = None
        self.fail_attempts = set()
        self.unknown = False
        self.missing_output = False
        self.missing_partition = False
        self.wrong_reduce_output = False
        self.rpc_error = False

    def _event(self, action, request, is_reduce):
        with self.events_lock:
            self.events.append((action, request.job_id, is_reduce, request.task_id))

    def _submit(self, request, is_reduce):
        with self.lock:
            if self.reject:
                return worker_pb2.TaskReply(accepted=False, message="Deliberate rejection")
            if self.current is not None:
                self.overlaps += 1
                return worker_pb2.TaskReply(accepted=False, message="Worker already busy")
            self.current = (request, is_reduce)
            self.calls.append((request, is_reduce))
            self.polls = 0
            self._event("start", request, is_reduce)
            return worker_pb2.TaskReply(accepted=True)

    def RunMap(self, request, context):
        return self._submit(request, False)

    def RunReduce(self, request, context):
        return self._submit(request, True)

    def GetTaskStatus(self, request, context):
        with self.lock:
            if self.rpc_error:
                context.abort(grpc.StatusCode.UNAVAILABLE, "Deliberate connection failure")
            if self.unknown or self.current is None:
                return worker_pb2.TaskStatusReply(state=worker_pb2.UNKNOWN)
            task, is_reduce = self.current
            if (request.job_id, request.task_id, request.attempt_id, request.is_reduce) != (
                    task.job_id, task.task_id, task.attempt_id, is_reduce):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Wrong task identity")
            self.polls += 1
            if not self.gate.is_set() or self.polls < 2:
                return worker_pb2.TaskStatusReply(state=worker_pb2.RUNNING)
            self.current = None
            if ((task.task_id, task.attempt_id, is_reduce) in self.fail_attempts
                    or (self.fail_reduce is not None and is_reduce == self.fail_reduce)):
                return worker_pb2.TaskStatusReply(state=worker_pb2.FAILED, error="Deliberate task failure")
            if is_reduce:
                path = Path(task.output_path) / f"reduce_{task.task_id}_attempt_{task.attempt_id}.jsonl"
                if self.wrong_reduce_output:
                    path = path.with_name("unexpected.jsonl")
                if task.intermediate_path or not task.input_paths:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Expected explicit partition files")
                if not all(Path(p).is_file() for p in task.input_paths):
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Missing input partition")
            else:
                path = self.root / task.job_id / f"map_{task.task_id}_attempt_{task.attempt_id}"
            if not self.missing_output:
                if is_reduce:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps({"task_id": task.task_id}) + "\n")
                else:
                    path.mkdir(parents=True, exist_ok=True)
                    for reducer_id in range(task.num_reducers):
                        if self.missing_partition and reducer_id == 0:
                            continue
                        partition = path / f"map_{task.task_id}_attempt_{task.attempt_id}_reduce_{reducer_id}.jsonl"
                        partition.write_text(json.dumps({"task_id": task.task_id}) + "\n")
            self._event("complete", task, is_reduce)
            return worker_pb2.TaskStatusReply(state=worker_pb2.COMPLETED, output_path=str(path))


class TestScheduler(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.input = self.root / "input.txt"
        self.input.write_text("a\nb\nc\nd\ne\nf\ng\n")
        self.job_file = self.root / "job.py"
        self.job_file.write_text("# Controlled Workers do not execute user code.\n")
        self.events = []
        self.events_lock = Lock()
        self.workers = []
        self.worker_servers = []
        addresses = []
        for index in range(2):
            worker = ControlledWorker(self.root / f"worker-{index}", self.events, self.events_lock)
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
            worker_grpc.add_WorkerServiceServicer_to_server(worker, server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            self.addCleanup(lambda server=server: server.stop(0).wait())
            self.workers.append(worker)
            self.worker_servers.append(server)
            addresses.append(f"127.0.0.1:{port}")
        self.store = JobStore()
        server, port = create_server("127.0.0.1:0", self.store)
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.addCleanup(channel.close)
        grpc.channel_ready_future(channel).result(timeout=5)
        self.stub = boss_pb2_grpc.BossServiceStub(channel)
        self.scheduler = Scheduler(
            self.store, addresses, self.root / "intermediate",
            poll_interval=0.01, rpc_timeout=0.2, task_timeout=5,
        )
        self.addCleanup(self.scheduler.stop)

    def submit(self, name="output", maps=4, reduces=2):
        return self.stub.SubmitJob(boss_pb2.SubmitJobRequest(
            input_path=str(self.input), output_path=str(self.root / name),
            job_file=str(self.job_file), num_maps=maps, num_reducers=reduces,
        ), timeout=5).job_id

    def status(self, job_id):
        return self.stub.GetJobStatus(boss_pb2.JobStatusRequest(job_id=job_id), timeout=5)

    def wait_for(self, predicate):
        deadline = monotonic() + 5
        while monotonic() < deadline:
            value = predicate()
            if value:
                return value
            Event().wait(0.01)
        self.fail("Timed out waiting for scheduler condition")

    def finished(self, job_id):
        def terminal():
            status = self.status(job_id)
            return status if status.state in (boss_pb2.JOB_COMPLETED, boss_pb2.JOB_FAILED) else None
        return self.wait_for(terminal)

    def test_dispatch_barrier_partition_selection_and_final_output(self):
        job_id = self.submit(maps=5, reduces=3)
        self.scheduler.start()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_COMPLETED, result.error)
        self.assertEqual((result.completed_maps, result.completed_reducers), (5, 3))
        calls = [call for worker in self.workers for call in worker.calls]
        maps = sorted((request for request, reduce in calls if not reduce), key=lambda r: r.task_id)
        self.assertEqual([(r.start_line, r.end_line) for r in maps], [(0, 2), (2, 4), (4, 5), (5, 6), (6, 7)])
        self.assertTrue(all(r.attempt_id == 0 and r.num_reducers == 3 for r in maps))
        for worker in self.workers:
            self.assertTrue(worker.calls, "Both Workers must receive work")
            self.assertEqual(worker.overlaps, 0)
        first_reduce = next(i for i, event in enumerate(self.events) if event[0] == "start" and event[2])
        completed_before_reduce = [e for e in self.events[:first_reduce] if e[0] == "complete" and not e[2]]
        self.assertEqual(len(completed_before_reduce), 5)
        reduces = [request for request, is_reduce in calls if is_reduce]
        for request in reduces:
            self.assertFalse(request.intermediate_path)
            self.assertEqual(len(request.input_paths), 5)
            self.assertEqual(
                [Path(path).name for path in request.input_paths],
                [f"map_{i}_attempt_0_reduce_{request.task_id}.jsonl" for i in range(5)],
            )
            self.assertTrue(all(Path(path).is_file() for path in request.input_paths))
            self.assertEqual(request.output_path, str(self.root / "intermediate" / job_id / "reduce"))
        parts = sorted((self.root / "output").iterdir())
        self.assertEqual([p.name for p in parts], [f"part-{i:05d}.jsonl" for i in range(3)])
        self.assertEqual([json.loads(p.read_text())["task_id"] for p in parts], [0, 1, 2])

    def test_speculation_chooses_duplicate_and_only_its_map_partitions(self):
        self.scheduler.speculate = True
        self.scheduler.straggler_seconds = 0.05
        # Worker 0 cannot finish map 0; Worker 1 becomes idle after map 1.
        self.workers[0].gate.clear()
        job_id = self.submit(maps=2, reduces=2)
        self.scheduler.start()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_COMPLETED, result.error)
        self.assertEqual((result.completed_maps, result.completed_reducers), (2, 2))
        self.assertEqual(
            [(r.task_id, r.attempt_id) for w in self.workers
             for r, is_reduce in w.calls if not is_reduce],
            [(0, 0), (1, 0), (0, 1)],
        )
        for w in self.workers:
            for request, is_reduce in w.calls:
                if is_reduce:
                    self.assertIn("map_0_attempt_1_reduce_", request.input_paths[0])
                    self.assertNotIn("map_0_attempt_0_reduce_", request.input_paths[0])
        # The losing original is still running; reducers must not use its Worker.
        self.assertEqual(self.workers[0].overlaps, 0)
        self.assertEqual(len(self.workers[0].calls), 1)

    def test_baseline_does_not_speculate_even_with_an_idle_worker(self):
        self.workers[0].gate.clear()
        job_id = self.submit(maps=2, reduces=1)
        self.scheduler.start()
        self.wait_for(lambda: self.status(job_id).completed_maps == 1)
        Event().wait(0.12)
        self.assertFalse(any(r.attempt_id == 1 for w in self.workers
                             for r, is_reduce in w.calls if not is_reduce))
        self.workers[0].gate.set()
        self.assertEqual(self.finished(job_id).state, boss_pb2.JOB_COMPLETED)

    def test_speculation_waits_for_peer_completion_and_reclaims_loser(self):
        self.scheduler.speculate = True
        self.scheduler.straggler_seconds = 0.05
        for worker in self.workers:
            worker.gate.clear()
        job_id = self.submit(maps=2, reduces=1)
        self.scheduler.start()
        Event().wait(0.12)
        self.assertFalse(any(r.attempt_id == 1 for w in self.workers
                             for r, is_reduce in w.calls if not is_reduce))
        self.workers[1].gate.set()
        self.assertEqual(self.finished(job_id).state, boss_pb2.JOB_COMPLETED)
        self.assertTrue(self.scheduler._lingering)
        self.workers[0].gate.set()
        next_job = self.submit("next", maps=2, reduces=1)
        self.assertEqual(self.finished(next_job).state, boss_pb2.JOB_COMPLETED)
        self.assertFalse(self.scheduler._lingering)
        self.assertEqual([w.overlaps for w in self.workers], [0, 0])

    def test_failed_speculative_attempt_can_fall_back_to_original(self):
        self.scheduler.speculate = True
        self.scheduler.straggler_seconds = 0.05
        self.workers[0].gate.clear()
        self.workers[1].fail_attempts.add((0, 1, False))
        job_id = self.submit(maps=2, reduces=1)
        self.scheduler.start()
        self.wait_for(lambda: any(
            r.task_id == 0 and r.attempt_id == 1
            for r, is_reduce in self.workers[1].calls if not is_reduce
        ))
        self.workers[0].gate.set()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_COMPLETED, result.error)
        self.assertEqual(result.completed_maps, 2)
        reducer_requests = [r for w in self.workers for r, is_reduce in w.calls if is_reduce]
        self.assertEqual(len(reducer_requests), 1)
        self.assertIn("map_0_attempt_0_reduce_", reducer_requests[0].input_paths[0])

    def test_progress_and_fifo_jobs(self):
        self.workers[0].gate.clear()
        first = self.submit("first", maps=2, reduces=1)
        second = self.submit("second", maps=1, reduces=1)
        self.scheduler.start()
        self.wait_for(lambda: self.status(first).completed_maps == 1)
        self.assertEqual(self.status(first).state, boss_pb2.JOB_RUNNING)
        self.assertEqual(self.status(second).state, boss_pb2.JOB_QUEUED)
        self.assertFalse(any(event[2] for event in self.events))
        self.workers[0].gate.set()
        self.assertEqual(self.finished(first).state, boss_pb2.JOB_COMPLETED)
        self.assertEqual(self.finished(second).state, boss_pb2.JOB_COMPLETED)
        job_order = [event[1] for event in self.events]
        self.assertLess(max(i for i, j in enumerate(job_order) if j == first),
                        min(i for i, j in enumerate(job_order) if j == second))

    def test_empty_input_still_schedules_requested_tasks(self):
        self.input.write_text("")
        job_id = self.submit(maps=3, reduces=2)
        self.scheduler.start()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_COMPLETED, result.error)
        maps = [r for w in self.workers for r, reduce in w.calls if not reduce]
        self.assertEqual(len(maps), 3)
        self.assertTrue(all(r.start_line == r.end_line == 0 for r in maps))

    def test_task_failure_blocks_reduce_and_later_job_can_run(self):
        self.workers[0].fail_reduce = False
        failed = self.submit("failed", maps=1)
        self.scheduler.start()
        result = self.finished(failed)
        self.assertEqual(result.state, boss_pb2.JOB_FAILED)
        self.assertIn("Deliberate task failure", result.error)
        self.assertFalse(any(event[2] for event in self.events))
        self.workers[0].fail_reduce = None
        succeeding = self.submit("succeeding")
        self.assertEqual(self.finished(succeeding).state, boss_pb2.JOB_COMPLETED)

    def test_failed_reduce_does_not_publish_partial_results(self):
        self.workers[1].fail_reduce = True
        job_id = self.submit()
        self.scheduler.start()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_FAILED)
        self.assertEqual(result.completed_maps, 4)
        self.assertEqual(result.completed_reducers, 1)
        self.assertEqual(list((self.root / "output").iterdir()), [])

    def test_timeout_disables_workers_with_uncertain_work(self):
        for worker in self.workers:
            worker.gate.clear()
        self.scheduler.task_timeout = 0.15
        first = self.submit("first", maps=2)
        second = self.submit("second", maps=2)
        self.scheduler.start()
        self.assertIn("timed out", self.finished(first).error)
        self.assertIn("No usable Workers", self.finished(second).error)
        self.assertEqual(sum(len(w.calls) for w in self.workers), 2)
        self.assertTrue(all(w.overlaps == 0 for w in self.workers))

    def test_rejection_fails_job(self):
        self.workers[0].reject = True
        job_id = self.submit()
        self.scheduler.start()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_FAILED)
        self.assertIn("rejected", result.error)

    def test_unknown_task_fails_job(self):
        self.workers[0].unknown = True
        job_id = self.submit()
        self.scheduler.start()
        self.assertIn("unknown state", self.finished(job_id).error)

    def test_rpc_failure_is_reported(self):
        self.workers[0].rpc_error = True
        job_id = self.submit()
        self.scheduler.start()
        self.assertIn("UNAVAILABLE", self.finished(job_id).error)

    def test_unreachable_worker_fails_job(self):
        self.worker_servers[0].stop(0).wait()
        job_id = self.submit()
        self.scheduler.start()
        self.assertIn("RPC failed", self.finished(job_id).error)

    def test_missing_output_fails_job(self):
        self.workers[0].missing_output = True
        job_id = self.submit()
        self.scheduler.start()
        self.assertIn("output path", self.finished(job_id).error)

    def test_missing_partition_fails_before_any_reducer_starts(self):
        self.workers[0].missing_partition = True
        job_id = self.submit()
        self.scheduler.start()
        result = self.finished(job_id)
        self.assertEqual(result.state, boss_pb2.JOB_FAILED)
        self.assertIn("missing reducer 0 partition", result.error)
        self.assertFalse(any(event[2] for event in self.events))

    def test_existing_output_is_preserved(self):
        output = self.root / "output"
        output.mkdir()
        saved = output / "previous-result"
        saved.write_text("keep me")
        job_id = self.submit()
        self.scheduler.start()
        self.assertIn("refusing to overwrite", self.finished(job_id).error)
        self.assertEqual(saved.read_text(), "keep me")
        self.assertFalse(any(worker.calls for worker in self.workers))

    def test_workspace_cannot_overlap_final_output(self):
        job_id = self.submit("intermediate")
        self.scheduler.start()
        self.assertIn("must not overlap", self.finished(job_id).error)
        self.assertFalse((self.root / "intermediate").exists())
        self.assertFalse(any(worker.calls for worker in self.workers))

    def test_reducer_must_return_its_assigned_output(self):
        self.workers[0].wrong_reduce_output = True
        job_id = self.submit()
        self.scheduler.start()
        self.assertIn("assigned reduce output", self.finished(job_id).error)
        self.assertEqual(list((self.root / "output").iterdir()), [])

    def test_filesystem_error_does_not_kill_scheduler(self):
        job_id = self.submit()
        self.input.unlink()
        self.scheduler.start()
        self.assertEqual(self.finished(job_id).state, boss_pb2.JOB_FAILED)
        self.input.write_text("restored\n")
        next_id = self.submit("next")
        self.assertEqual(self.finished(next_id).state, boss_pb2.JOB_COMPLETED)

    def test_shutdown_fails_active_job_without_starting_reducers(self):
        for worker in self.workers:
            worker.gate.clear()
        job_id = self.submit()
        self.scheduler.start()
        self.wait_for(lambda: len(self.events) == 2)
        self.scheduler.stop()
        self.assertEqual(self.status(job_id).state, boss_pb2.JOB_FAILED)
        self.assertIn("stopped", self.status(job_id).error)
        self.assertFalse(any(event[2] for event in self.events))


class TestJobLifecycle(unittest.TestCase):
    def test_task_counts_and_terminal_states(self):
        store = JobStore()
        job = store.create(JobSpec("/input", "/output", "/job.py", 1, 1))
        self.assertEqual(store.claim_next().job_id, job.job_id)
        with self.assertRaises(ValueError):
            store.task_completed(job.job_id, 0, True)
        with self.assertRaises(ValueError):
            store.task_completed(job.job_id, 1, False)
        with self.assertRaises(ValueError):
            store.complete(job.job_id)
        store.task_completed(job.job_id, 0, False)
        store.task_completed(job.job_id, 0, False)
        self.assertEqual(store.get(job.job_id).completed_maps, 1)
        store.task_completed(job.job_id, 0, True)
        store.complete(job.job_id)
        store.fail(job.job_id, "late error")
        self.assertEqual(store.get(job.job_id).state, boss_pb2.JOB_COMPLETED)
        self.assertIsNone(store.claim_next())

    def test_line_ranges_cover_input_once(self):
        for count, maps in ((0, 4), (2, 5), (7, 3), (8, 1)):
            with self.subTest(lines=count, maps=maps):
                ranges = line_ranges(count, maps)
                self.assertEqual(len(ranges), maps)
                self.assertEqual([i for start, end in ranges for i in range(start, end)], list(range(count)))
                sizes = [end - start for start, end in ranges]
                self.assertLessEqual(max(sizes) - min(sizes), 1)


if __name__ == "__main__":
    unittest.main()
