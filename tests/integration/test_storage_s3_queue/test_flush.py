"""
Tests for SYSTEM FLUSH OBJECT STORAGE QUEUE db.table PATH 'x'.

The command must block until the given path is marked as processed (or failed)
in Keeper by the S3Queue background thread, then return.
"""

import logging
import threading

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_mv,
    create_table,
    generate_random_files,
    generate_random_string,
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


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_waits_until_path_is_processed(started_cluster, mode):
    """
    SYSTEM FLUSH OBJECT STORAGE QUEUE must return only after the background
    thread has committed the given file to the processed set in Keeper.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_wait_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"

    generate_random_files(started_cluster, files_path, count=1, row_num=5)
    # Derive the exact S3 key that was uploaded.
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

    # Run the flush command synchronously — it must block until the file is processed.
    node.query(f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'")

    # By the time FLUSH returns the file must be in the destination table.
    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count == 5, f"Expected 5 rows after flush, got {count}"


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_returns_immediately_if_already_processed(started_cluster, mode):
    """
    If the file was already processed before FLUSH is called, the command must
    return immediately without blocking.
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
        import time
        time.sleep(1)
    else:
        raise AssertionError("File was not processed within timeout")

    # FLUSH must succeed immediately since the file is already processed.
    node.query(f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'")


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_unblocks_after_background_processing(started_cluster, mode):
    """
    Upload a file, issue FLUSH concurrently, and verify that FLUSH returns
    only after the background thread finishes — not before.

    The check is done by asserting that the destination table is non-empty
    (i.e. the INSERT happened) by the time FLUSH returns.
    """

    node = started_cluster.instances["instance"]
    table_name = f"flush_concurrent_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"

    generate_random_files(started_cluster, files_path, count=1, row_num=8)
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

    flush_done = threading.Event()
    flush_error = []

    def run_flush():
        try:
            node.query(
                f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'"
            )
        except Exception as e:
            flush_error.append(e)
        finally:
            flush_done.set()

    t = threading.Thread(target=run_flush)
    t.start()

    # FLUSH must complete within a reasonable time.
    assert flush_done.wait(timeout=60), "SYSTEM FLUSH OBJECT STORAGE QUEUE did not return within 60 seconds"
    t.join()

    if flush_error:
        raise flush_error[0]

    # After FLUSH returns the row must be in the destination table.
    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count == 8, f"Expected 8 rows after flush, got {count}"
