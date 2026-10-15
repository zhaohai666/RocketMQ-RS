import argparse
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import tempfile
import threading
import time


def run(command, timeout=120):
    result = subprocess.run(
        [str(part) for part in command],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )
    if result.stdout.strip():
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    result.check_returncode()
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description="Verify Rust services with original Java client jars")
    parser.add_argument("--classpath", required=True, help="Original RocketMQ distribution lib/* path")
    parser.add_argument("--java-home", type=Path, help="JDK directory containing bin/java and bin/javac")
    root = Path(__file__).resolve().parent.parent
    parser.add_argument("--rocketmq", type=Path, default=root / "target/debug/rocketmq")
    args = parser.parse_args()
    java = args.java_home / "bin/java" if args.java_home else "java"
    javac = args.java_home / "bin/javac" if args.java_home else "javac"
    with tempfile.TemporaryDirectory(prefix="rocketmq-live-") as temporary:
        directory = Path(temporary)
        classes = directory / "classes"
        classes.mkdir()
        run([javac, "--release", "8", "-encoding", "UTF-8", "-cp", args.classpath,
             "-d", classes, root / "tests/JavaInterop.java"])
        process = subprocess.Popen(
            [str(args.rocketmq), "standalone", "--namesrv-listen", "127.0.0.1:0",
             "--broker-listen", "127.0.0.1:0", "--recall-message-enable", "true",
             "--data-dir", str(directory / "data")],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, "RUST_LOG": "info"},
        )
        lines = queue.Queue()

        def read_logs():
            for line in process.stdout:
                clean = re.sub(r"\x1b\[[0-9;]*m", "", line)
                print(clean, end="")
                lines.put(clean)
            lines.put(None)

        reader = threading.Thread(target=read_logs, daemon=True)
        reader.start()
        try:
            addresses = {}
            deadline = time.monotonic() + 20
            while True:
                line = lines.get(timeout=max(0, deadline - time.monotonic()))
                if line is None:
                    raise RuntimeError("server stopped before registration")
                match = re.search(r'service="(namesrv|broker)"\s+listen=(127\.0\.0\.1:\d+)', line)
                if match:
                    addresses[match.group(1)] = match.group(2)
                if "Broker registered with every configured NameServer" in line:
                    break
            if set(addresses) != {"namesrv", "broker"}:
                raise RuntimeError("server did not report both bound addresses")
            for serialization in ["JSON", "ROCKETMQ"]:
                output = run([java, "-Duser.home=" + temporary,
                              "-Drocketmq.serialize.type=" + serialization,
                              "-cp", str(classes) + os.pathsep + args.classpath,
                              "JavaInterop", addresses["namesrv"], "broker-a"])
                if "JAVA_INTEROP_OK" not in output:
                    raise RuntimeError("Java interoperability success marker missing")
                print("JAVA_SERIALIZATION_OK", serialization)
            commands = [
                ["cluster-list", "--namesrv", addresses["namesrv"]],
                ["runtime", "--broker", addresses["broker"]],
                ["topic-create", "--broker", addresses["broker"], "--topic", "CliCreated", "--queues", "2"],
                ["topic-route", "--namesrv", addresses["namesrv"], "--topic", "CliCreated"],
            ]
            for command in commands:
                run([args.rocketmq, "admin", *command], timeout=10)
                print("ADMIN_OK", command[0])
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError("server failed to stop after SIGTERM") from None
            reader.join(timeout=2)
            process.stdout.close()
        if process.returncode != 0:
            raise RuntimeError("server exited unsuccessfully: " + str(process.returncode))
    print("JAVA_AND_CLI_INTEROP_OK")


if __name__ == "__main__":
    main()
