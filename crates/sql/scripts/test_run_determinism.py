import fcntl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from run_determinism import OutputBudget, run_process


class ProcessLifetimeTests(unittest.TestCase):
    def test_machine_results_and_error_diagnostics_use_separate_streams(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                (root / "values.jsonl").open("w") as values,
                (root / "errors.log").open("w") as errors,
            ):
                run_process(
                    [
                        sys.executable,
                        "-c",
                        (
                            'import sys; print(\'{"status":"runtime_error"}\'); '
                            "print('expected panic diagnostic', file=sys.stderr)"
                        ),
                    ],
                    root,
                    values,
                    5,
                    threading.Event(),
                    OutputBudget(root, 4096),
                    stderr=errors,
                )
            self.assertEqual(
                (root / "values.jsonl").read_text(), '{"status":"runtime_error"}\n'
            )
            self.assertEqual(
                (root / "errors.log").read_text(), "expected panic diagnostic\n"
            )

    def test_low_space_prevents_starting_a_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "must-not-exist"
            budget = OutputBudget(root, 4096, [root], min_free=2048)
            with (
                patch("run_determinism.shutil.disk_usage") as usage,
                (root / "process.log").open("w") as log,
                self.assertRaisesRegex(RuntimeError, "Free space"),
            ):
                usage.return_value.free = 1024
                run_process(
                    [sys.executable, "-c", f"open({str(artifact)!r}, 'w').close()"],
                    directory,
                    log,
                    5,
                    threading.Event(),
                    budget,
                )
            self.assertFalse(artifact.exists())

    def test_output_limit_kills_writer_and_preserves_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "artifact"
            lock = root / "writer.lock"
            writer = (
                "import fcntl, pathlib, signal, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"lock = open({str(lock)!r}, 'w'); "
                "fcntl.flock(lock, fcntl.LOCK_EX); "
                f"pathlib.Path({str(artifact)!r}).write_bytes(b'x' * 2048); "
                "time.sleep(60)"
            )
            with (
                (root / "process.log").open("w") as log,
                self.assertRaisesRegex(RuntimeError, "output exceeds"),
            ):
                run_process(
                    [sys.executable, "-c", writer],
                    directory,
                    log,
                    5,
                    threading.Event(),
                    OutputBudget(root, 1024),
                )
            self.assertEqual(2048, artifact.stat().st_size)
            with lock.open("w") as resource:
                fcntl.flock(resource, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_output_limit_checks_completed_fast_writers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                (root / "process.log").open("w") as log,
                self.assertRaisesRegex(RuntimeError, "output exceeds"),
            ):
                run_process(
                    [sys.executable, "-c", "print('x' * 2048)"],
                    directory,
                    log,
                    5,
                    threading.Event(),
                    OutputBudget(root, 1024),
                )

    def test_nonzero_exit_and_timeout_are_not_success(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            (Path(directory) / "process.log").open("w") as log,
        ):
            for code, timeout, error in (
                ("raise SystemExit(7)", 5, subprocess.CalledProcessError),
                ("import time; time.sleep(60)", 0.2, subprocess.TimeoutExpired),
            ):
                with self.assertRaises(error):
                    run_process(
                        [sys.executable, "-c", code],
                        directory,
                        log,
                        timeout,
                        threading.Event(),
                    )

    def test_cancellation_releases_resources_held_by_descendants(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "child-ready"
            lock = root / "child.lock"
            child = (
                "import fcntl, pathlib, time; "
                f"lock = open({str(lock)!r}, 'w'); "
                "fcntl.flock(lock, fcntl.LOCK_EX); "
                f"pathlib.Path({str(ready)!r}).touch(); "
                "time.sleep(60)"
            )
            parent = (
                "import subprocess, sys; "
                f"subprocess.run([sys.executable, '-c', {child!r}], check=True)"
            )
            cancelled = threading.Event()

            def cancel_when_ready():
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    if cancelled.wait(0.01):
                        return
                cancelled.set()

            cancellation = threading.Thread(target=cancel_when_ready)
            cancellation.start()
            try:
                with (
                    (root / "process.log").open("w") as log,
                    self.assertRaisesRegex(RuntimeError, "cancelled"),
                ):
                    run_process(
                        [sys.executable, "-c", parent], directory, log, 10, cancelled
                    )
                self.assertTrue(ready.exists(), "Child never acquired its resource")
                # A killed descendant releases its advisory lock, even if it has
                # not yet been reaped by the container's init process.
                with lock.open("w") as resource:
                    fcntl.flock(resource, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                cancelled.set()
                cancellation.join()


if __name__ == "__main__":
    unittest.main()
