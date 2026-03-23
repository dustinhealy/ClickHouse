"""
Tests for SYSTEM FLUSH OBJECT STORAGE QUEUE db.table PATH 'x'.

The command must block until the given path is marked as processed (or failed)
in Keeper by the S3Queue background thread, then return.

Blocking is proven deterministically by using the `object_storage_queue_fail_commit`
failpoint: while the failpoint is active the background worker can never write a
`Processed` node in Keeper, so FLUSH cannot return.  The moment the failpoint is
disabled the next commit succeeds and FLUSH unblocks.
"""

import logging
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_mv,
    create_table,
    generate_random_files,
    generate_random_string,
    put_s3_file_content,
)

AVAILABLE_MODES = ["unordered", "ordered"]


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")

    minio = started_cluster.minio_client
    for obj in minio.list_objects(started_cluster.minio_bucket, recursive=True):
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)

    yield


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


def _run_flush_in_thread(node, table_name, file_path):
    """Start FLUSH in a daemon thread; return (thread, done_event, error_list)."""
    done = threading.Event()
    errors = []

    def _target():
        try:
            node.query(
                f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name}"
                f" PATH '{file_path}'"
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            done.set()

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    return t, done, errors


def _wait_for_commit_failure(node, table_name, timeout_s=60):
    """
    Spin until the background thread has hit `fail_commit` at least once.
    This is the signal that the worker has the file, has inserted the data,
    and is now stuck trying (and failing) to write the `Processed` node to Keeper.
    """
    msg = (
        f"StorageS3Queue (default.{table_name}): Failed to process data:"
        f" Code: 999. Coordination::Exception: Failed to commit processed files"
    )
    for _ in range(timeout_s):
        if node.contains_in_log(msg):
            return
        time.sleep(1)
    raise AssertionError(
        f"Background thread did not hit fail_commit within {timeout_s} s"
    )


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_blocks_until_commit_succeeds(started_cluster, mode):
    """
    Use `object_storage_queue_fail_commit` to pin the background worker in a
    permanent retry loop.  While the failpoint is active the file can never
    reach `Processed` state, so FLUSH must block.  Once the failpoint is
    disabled the commit succeeds and FLUSH must unblock.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_block_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/test_0.csv"

    generate_random_files(started_cluster, files_path, count=1, row_num=5)

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    try:
        create_table(
            started_cluster,
            node,
            table_name,
            mode,
            files_path,
            additional_settings={"keeper_path": keeper_path},
        )
        create_mv(node, table_name, dst_table_name)

        # Wait until the background thread has hit the failpoint at least once.
        # The file is now stuck in the "picked up but cannot commit" cycle.
        _wait_for_commit_failure(node, table_name)

        # Start FLUSH while the failpoint is still active.
        t, flush_done, flush_errors = _run_flush_in_thread(node, table_name, file_path)

        # Allow the FLUSH query to reach the server and register its Keeper
        # watches.  After this sleep the file is still unprocessable (fail_commit
        # is active), so done must still be unset.
        time.sleep(1.0)

        assert not flush_done.is_set(), (
            "FLUSH returned while fail_commit was still active — it did not block"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    # With the failpoint gone the next commit will succeed; FLUSH must unblock.
    assert flush_done.wait(timeout=120), (
        "FLUSH did not unblock within 120 s after disabling fail_commit"
    )
    t.join()

    if flush_errors:
        raise flush_errors[0]

    # The file was reinserted on every failed retry, so the count may exceed
    # row_num; we only assert that at least one successful processing occurred.
    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count > 0, f"Expected rows in destination table after flush, got 0"


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_returns_quickly_if_already_processed(started_cluster, mode):
    """
    If the file was already processed before FLUSH is called, the command must
    return well within the watch-timeout period (< 5 s).
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_already_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"

    generate_random_files(started_cluster, files_path, count=1, row_num=3)
    file_path = f"{files_path}/test_0.csv"

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )
    create_mv(node, table_name, dst_table_name)

    # Wait for the background thread to process the file naturally.
    for _ in range(60):
        if int(node.query(f"SELECT count() FROM {dst_table_name}")) == 3:
            break
        time.sleep(1)
    else:
        raise AssertionError("File was not processed within 60 s")

    # FLUSH must see `Processed` on the first Keeper read and return immediately.
    deadline = time.monotonic() + 5.0
    node.query(
        f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'"
    )
    assert time.monotonic() < deadline, (
        "SYSTEM FLUSH took too long for an already-processed file"
    )


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_raises_on_failed_path(started_cluster, mode):
    """
    When the background thread permanently fails a file (retries exhausted),
    FLUSH must raise an exception (error code ABORTED) instead of waiting forever.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_fail_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/bad.csv"

    # Upload a file whose first column cannot be parsed as UInt32.
    bad_csv = b"not_a_number,1,2\n"
    put_s3_file_content(started_cluster, file_path, bad_csv)

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,
        },
    )
    create_mv(node, table_name, dst_table_name)

    # Wait until Keeper shows the file as permanently failed.
    zk = started_cluster.get_kazoo_client("zoo1")
    for _ in range(60):
        try:
            failed_nodes = zk.get_children(f"{keeper_path}/failed/")
            if failed_nodes:
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise AssertionError("File was not marked as failed within 60 s")

    # FLUSH must raise rather than hang.
    error = node.query_and_get_error(
        f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'"
    )
    assert error, "Expected FLUSH to raise an error for a permanently failed file"
    assert "ABORTED" in error or "failed" in error.lower(), (
        f"Unexpected error message: {error}"
    )


def test_flush_ordered_with_buckets(started_cluster):
    """
    FLUSH must work correctly for ordered-mode queues that use multiple buckets.
    Each bucket has its own processed pointer and the watch must land on the right
    per-bucket node.  `object_storage_queue_fail_commit` is used so that FLUSH is
    guaranteed to be blocking when the assertion is made.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_buckets_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/test_0.csv"

    generate_random_files(started_cluster, files_path, count=1, row_num=4)

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    try:
        create_table(
            started_cluster,
            node,
            table_name,
            "ordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "buckets": 4,
            },
        )
        create_mv(node, table_name, dst_table_name)

        _wait_for_commit_failure(node, table_name)

        t, flush_done, flush_errors = _run_flush_in_thread(node, table_name, file_path)
        time.sleep(1.0)

        assert not flush_done.is_set(), (
            "FLUSH returned while fail_commit was still active — it did not block"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    assert flush_done.wait(timeout=120), (
        "FLUSH (buckets=4) did not unblock within 120 s after disabling fail_commit"
    )
    t.join()

    if flush_errors:
        raise flush_errors[0]

    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count > 0, f"Expected rows in destination table after flush, got 0"
