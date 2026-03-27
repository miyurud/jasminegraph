#!/usr/bin/env python3
"""
Ultra-high performance Kafka publisher for billion-scale edge streaming
Optimized for maximum throughput with configurable reliability levels

Performance modes:
- BALANCED: ~100K-500K edges/sec (some reliability)
- FAST: ~500K-2M edges/sec (minimal reliability)
- EXTREME: ~2M-10M+ edges/sec (fire-and-forget with multiprocessing)

Usage:
    python3 stream_to_kafka_ultra.py <input_file> <topic> [mode] [num_workers] [bootstrap_server] [input_format]
  
Examples:
    # Balanced mode (default, JSON lines)
  python3 stream_to_kafka_ultra.py edges.json my_topic balanced
  
  # Fast mode with 4 parallel workers
  python3 stream_to_kafka_ultra.py edges.json my_topic fast 4
  
    # Extreme mode with 8 parallel workers
  python3 stream_to_kafka_ultra.py edges.json my_topic extreme 8

    # CSV input (source,target columns)
    python3 stream_to_kafka_ultra.py edges.csv my_topic extreme 1 172.28.5.8:9092 csv
"""

import sys
import json
import csv
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from multiprocessing import Process, Value, Queue
from queue import Empty
import ctypes




class PerformanceMode:
    """Configuration for different performance modes"""
    
    BALANCED = {
        'name': 'BALANCED',
        'acks': 1,  # Leader acknowledges
        'max_in_flight': 5,
        'buffer_memory': 134217728,  # 128MB
        'batch_size': 262144,  # 256KB
        'linger_ms': 20,
        'compression': 'lz4',  # Faster than gzip
        'flush_interval': 100000,
        'expected_rate': '100K-500K edges/sec'
    }

    FAST = {
        'name': 'FAST',
        'acks': 1,  # Leader acknowledgment (reliable)
        'max_in_flight': 10,
        'buffer_memory': 268435456,  # 256MB
        'batch_size': 524288,  # 512KB
        'linger_ms': 50,
        'compression': 'lz4',
        'flush_interval': 500000,
        'expected_rate': '500K-2M edges/sec'
    }

    EXTREME = {
        'name': 'EXTREME',
        'acks': 1,  # Leader acknowledgment (fast + reliable)
        'max_in_flight': 20,
        'buffer_memory': 536870912,  # 512MB
        'batch_size': 1048576,  # 1MB
        'linger_ms': 100,
        'compression': 'lz4',  # Very fast
        'flush_interval': 50000,  # Flush every 50K for good balance
        'expected_rate': '50K-500K edges/sec (multiprocess, reliable)'
    }


def resolve_input_format(file_path, input_format_arg):
    """Resolve input format from argument or file extension."""
    if input_format_arg in ('json', 'jsonl', 'csv'):
        return 'csv' if input_format_arg == 'csv' else 'json'

    lower = file_path.lower()
    if lower.endswith('.csv'):
        return 'csv'
    return 'json'


def build_edge_from_csv_values(source_id, target_id):
    """Build edge JSON object expected by downstream StreamHandler."""
    return {
        "source": {
            "id": source_id,
            "properties": {
                "name": f"Node_{source_id}"
            }
        },
        "destination": {
            "id": target_id,
            "properties": {
                "name": f"Node_{target_id}"
            }
        },
        "properties": {
            "publication": "1",
            "title": "Wikipedia",
            "graphId": "1"
        }
    }


def parse_csv_line_to_edge(line):
    """Fast CSV parser for source,target rows."""
    text = line.strip()
    if not text:
        return None

    parts = text.split(',', 2)
    if len(parts) < 2:
        return None

    source_id = parts[0].strip().strip('"')
    target_id = parts[1].strip().strip('"')

    if not source_id or not target_id:
        return None

    if source_id.lower() == 'source' and target_id.lower() == 'target':
        return None

    return build_edge_from_csv_values(source_id, target_id)


def iterate_csv_edges(file_path):
    """Yield edges from CSV file with header or plain source,target rows."""
    with open(file_path, 'r', buffering=1024 * 1024) as f:
        for line in f:
            edge = parse_csv_line_to_edge(line)
            if edge is not None:
                yield edge


def get_csv_data_start_offset(file_path):
    """Return byte offset after optional CSV header line."""
    with open(file_path, 'rb') as f:
        first = f.readline()
        if not first:
            return 0
        try:
            first_text = first.decode('utf-8', errors='ignore').strip()
        except Exception:
            return 0

        cols = first_text.split(',', 2)
        if len(cols) >= 2 and cols[0].strip().strip('"').lower() == 'source' and \
                cols[1].strip().strip('"').lower() == 'target':
            return f.tell()
    return 0


def build_csv_byte_chunks(file_path, num_workers):
    """Split CSV file into newline-aligned byte ranges for parallel workers."""
    data_start = get_csv_data_start_offset(file_path)
    return build_newline_byte_chunks(file_path, num_workers, data_start)


def build_newline_byte_chunks(file_path, num_workers, data_start=0):
    """Split any line-oriented file into newline-aligned byte ranges for workers."""
    total_size = os.path.getsize(file_path)
    if total_size <= data_start:
        return []

    data_size = total_size - data_start
    chunks = []

    with open(file_path, 'rb') as f:
        for i in range(num_workers):
            raw_start = data_start + (data_size * i) // num_workers
            raw_end = data_start + (data_size * (i + 1)) // num_workers

            if i == 0:
                start = data_start
            else:
                f.seek(raw_start)
                f.readline()  # discard partial line
                start = f.tell()

            if i == num_workers - 1:
                end = total_size
            else:
                f.seek(raw_end)
                f.readline()  # advance to next full line boundary
                end = f.tell()

            if start < end:
                chunks.append((start, end))

    return chunks


def normalize_json_payload_bytes(raw_line):
    """Normalize a JSON line payload for direct Kafka publish as bytes."""
    payload = raw_line.strip()
    if not payload:
        return None

    # Support JSON array files with one object per line and trailing commas.
    if payload in (b'[', b']'):
        return None
    if payload.endswith(b','):
        payload = payload[:-1].rstrip()
        if not payload:
            return None

    return payload


def get_topic_partition_count(bootstrap_server, topic):
    """Auto-discover the number of Kafka partitions for a topic."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server],
                                 request_timeout_ms=5000)
        partitions = consumer.partitions_for_topic(topic)
        consumer.close()
        count = len(partitions) if partitions else 1
        print(f"  Topic '{topic}' has {count} Kafka partition(s).")
        return count
    except Exception as e:
        print(f"  WARNING: could not discover partition count ({e}), defaulting to 1")
        return 1


def send_end_of_stream_to_all_partitions(bootstrap_server, topic):
    """
    Send the -1 sentinel to EVERY Kafka partition so that every consumer
    thread (one per partition) receives the stop signal and drains its
    own slot completely before exiting.
    Without this, only one partition gets -1 and the rest may still
    hold unconsumed edges that get silently dropped.
    """
    num_partitions = get_topic_partition_count(bootstrap_server, topic)
    print(f"  Sending end-of-stream signal to all {num_partitions} Kafka partition(s)...")
    eos_producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    for pid in range(num_partitions):
        eos_producer.send(topic, value=b'-1', partition=pid)
    eos_producer.flush(timeout=10)
    eos_producer.close(timeout=5)
    print(f"  End-of-stream sent to {num_partitions} partition(s).")


def create_producer(bootstrap_server, config):
    """Create Kafka producer with given configuration"""
    return KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else json.dumps(v, separators=(',', ':')).encode('utf-8'),
        acks=config['acks'],
        retries=3 if config['acks'] > 0 else 0,
        max_in_flight_requests_per_connection=config['max_in_flight'],
        buffer_memory=config['buffer_memory'],
        batch_size=config['batch_size'],
        linger_ms=config['linger_ms'],
        compression_type=config['compression'],
        request_timeout_ms=30000,
        max_request_size=2097152,  # 2MB
    )


def worker_process(worker_id, file_path, topic, bootstrap_server, config,
                   start_line, end_line, counter, error_queue,
                   input_format='json', start_offset=0, end_offset=0):
    """Worker process for parallel streaming"""
    try:
        producer = create_producer(bootstrap_server, config)
        edge_count = 0
        error_count = 0
        counter_flush_interval = 10000
        counter_pending = 0

        if input_format == 'csv':
            with open(file_path, 'rb', buffering=4 * 1024 * 1024) as f:
                f.seek(start_offset)
                while True:
                    pos = f.tell()
                    if pos >= end_offset:
                        break
                    raw = f.readline()
                    if not raw:
                        break

                    try:
                        line = raw.decode('utf-8', errors='ignore')
                        edge = parse_csv_line_to_edge(line)
                        if edge is None:
                            continue

                        producer.send(topic, value=edge)
                        edge_count += 1

                        if edge_count % config['flush_interval'] == 0:
                            producer.flush()

                        counter_pending += 1
                        if counter_pending >= counter_flush_interval:
                            with counter.get_lock():
                                counter.value += counter_pending
                            counter_pending = 0
                    except Exception as e:
                        error_count += 1
                        if error_count <= 10:
                            error_queue.put(f"Worker {worker_id}: Byte {pos}: {str(e)}")
        else:
            with open(file_path, 'rb', buffering=4 * 1024 * 1024) as f:
                f.seek(start_offset)
                while True:
                    pos = f.tell()
                    if pos >= end_offset:
                        break
                    raw = f.readline()
                    if not raw:
                        break
                    try:
                        payload = normalize_json_payload_bytes(raw)
                        if payload is None:
                            continue

                        producer.send(topic, value=payload)
                        edge_count += 1

                        # Periodic flush
                        if edge_count % config['flush_interval'] == 0:
                            producer.flush()

                        counter_pending += 1
                        if counter_pending >= counter_flush_interval:
                            with counter.get_lock():
                                counter.value += counter_pending
                            counter_pending = 0

                    except Exception as e:
                        error_count += 1
                        if error_count <= 10:  # Limit error messages
                            error_queue.put(f"Worker {worker_id}: Byte {pos}: {str(e)}")
        
        # Final flush with acks=1 ensures delivery
        producer.flush(timeout=60)
        producer.close(timeout=10)
        
        # Update shared progress counter with any remaining local count
        with counter.get_lock():
            counter.value += counter_pending
            
    except Exception as e:
        error_queue.put(f"Worker {worker_id} failed: {str(e)}")


def count_lines(file_path):
    """Quickly count lines in file"""
    count = 0
    with open(file_path, 'rb') as f:
        for _ in f:
            count += 1
    return count


def stream_edges_single(file_path, topic, bootstrap_server, config, input_format='json'):
    """Single-threaded streaming with optimized settings"""
    
    print(f"╔{'═'*70}╗")
    print(f"║ {'HIGH-PERFORMANCE KAFKA STREAMING':^68} ║")
    print(f"╠{'═'*70}╣")
    print(f"║ Mode: {config['name']:<63} ║")
    print(f"║ Expected: {config['expected_rate']:<60} ║")
    print(f"╠{'═'*70}╣")
    print(f"║ File:   {file_path:<61} ║")
    print(f"║ Topic:  {topic:<61} ║")
    print(f"║ Server: {bootstrap_server:<61} ║")
    print(f"╠{'═'*70}╣")
    print(f"║ {'Configuration':<68} ║")
    print(f"║   acks={config['acks']}, in_flight={config['max_in_flight']}, batch={config['batch_size']//1024}KB, linger={config['linger_ms']}ms, compression={config['compression']:<6} ║")
    print(f"╚{'═'*70}╝")
    print()
    
    producer = create_producer(bootstrap_server, config)
    
    edge_count = 0
    start_time = time.time()
    last_report = start_time
    last_count = 0
    
    try:
        if input_format == 'csv':
            edge_iterator = iterate_csv_edges(file_path)
        else:
            with open(file_path, 'rb') as f:
                edge_iterator = (payload for payload in (normalize_json_payload_bytes(line) for line in f)
                                 if payload is not None)

        for edge in edge_iterator:
            producer.send(topic, value=edge)
            edge_count += 1

            # Periodic flush
            if edge_count % config['flush_interval'] == 0:
                producer.flush()

            # Progress report
            if edge_count % 100000 == 0:
                current_time = time.time()
                interval_time = current_time - last_report
                interval_edges = edge_count - last_count
                current_rate = interval_edges / interval_time

                elapsed = current_time - start_time
                avg_rate = edge_count / elapsed

                print(f"  ⚡ {edge_count:>10,} edges | "
                      f"Current: {current_rate:>8,.0f} edges/sec | "
                      f"Average: {avg_rate:>8,.0f} edges/sec")

                last_report = current_time
                last_count = edge_count
    except Exception as e:
        if edge_count < 10:
            print(f"  ⚠️  Parse error after {edge_count} edges: {e}")
    
    # Final flush - ensure all messages are sent
    print("\n  Flushing remaining messages...")
    producer.flush(timeout=60)
    producer.close(timeout=10)
    
    # Send -1 to EVERY Kafka partition so every consumer thread sees it
    # and drains its own slot before stopping.
    send_end_of_stream_to_all_partitions(bootstrap_server, topic)

    total_time = time.time() - start_time
    avg_rate = edge_count / total_time
    
    print()
    print(f"╔{'═'*70}╗")
    print(f"║ {'STREAMING COMPLETED':^68} ║")
    print(f"╠{'═'*70}╣")
    print(f"║ Total edges:    {edge_count:>10,} edges{' '*38} ║")
    print(f"║ Time taken:     {total_time:>10.1f} seconds{' '*36} ║")
    print(f"║ Average rate:   {avg_rate:>10,.0f} edges/sec{' '*32} ║")
    
    # Estimates for different scales
    time_1m = 1000000 / avg_rate
    time_10m = 10000000 / avg_rate
    time_100m = 100000000 / avg_rate
    time_1b = 1000000000 / avg_rate
    
    print(f"╠{'═'*70}╣")
    print(f"║ {'Time Estimates':<68} ║")
    print(f"║   1M edges:     {time_1m:>10.1f} seconds = {time_1m/60:>6.1f} min{' '*23} ║")
    print(f"║   10M edges:    {time_10m:>10.1f} seconds = {time_10m/60:>6.1f} min{' '*23} ║")
    print(f"║   100M edges:   {time_100m:>10.1f} seconds = {time_100m/3600:>6.1f} hours{' '*21} ║")
    print(f"║   1B edges:     {time_1b:>10.1f} seconds = {time_1b/3600:>6.1f} hours{' '*21} ║")
    print(f"╚{'═'*70}╝")


def stream_edges_parallel(file_path, topic, bootstrap_server, config, num_workers, input_format='json'):
    """Multi-process parallel streaming for maximum throughput"""
    
    print(f"╔{'═'*70}╗")
    print(f"║ {'PARALLEL HIGH-PERFORMANCE KAFKA STREAMING':^68} ║")
    print(f"╠{'═'*70}╣")
    print(f"║ Mode:    {config['name']:<60} ║")
    print(f"║ Workers: {num_workers:<60} ║")
    print(f"║ Expected: {config['expected_rate']:<59} ║")
    print(f"╚{'═'*70}╝")
    print()
    
    total_lines = 0
    byte_chunks = None

    if input_format == 'csv':
        print("  Building newline-safe CSV byte chunks...")
        byte_chunks = build_csv_byte_chunks(file_path, num_workers)
        if not byte_chunks:
            print("  No CSV data rows found.")
            return
        num_workers = len(byte_chunks)
        for i, (s, e) in enumerate(byte_chunks):
            print(f"    Worker {i}: bytes {s:,} to {e:,}")
    else:
        print("  Building newline-safe JSON byte chunks...")
        byte_chunks = build_newline_byte_chunks(file_path, num_workers, 0)
        if not byte_chunks:
            print("  No JSON data rows found.")
            return
        num_workers = len(byte_chunks)
        for i, (s, e) in enumerate(byte_chunks):
            print(f"    Worker {i}: bytes {s:,} to {e:,}")

        # Count lines only for progress percentage display.
        print("  Counting edges...")
        total_lines = count_lines(file_path)
        print(f"  Total edges: {total_lines:,}")

    workers = []
    counter = Value(ctypes.c_long, 0)
    error_queue = Queue()
    
    print(f"\n  Starting {num_workers} parallel workers...")
    start_time = time.time()
    
    for i in range(num_workers):
        start_offset, end_offset = byte_chunks[i]
        start_line = 0
        end_line = 0

        p = Process(
            target=worker_process,
            args=(i, file_path, topic, bootstrap_server, config,
                  start_line, end_line, counter, error_queue,
                  input_format, start_offset, end_offset)
        )
        p.start()
        workers.append(p)
    
    # Monitor progress
    print("\n  Streaming in progress...")
    last_count = 0
    last_time = start_time
    
    while any(w.is_alive() for w in workers):
        time.sleep(5)
        
        current_count = counter.value
        current_time = time.time()
        
        interval_edges = current_count - last_count
        interval_time = current_time - last_time
        current_rate = interval_edges / interval_time if interval_time > 0 else 0
        
        elapsed = current_time - start_time
        avg_rate = current_count / elapsed if elapsed > 0 else 0

        if total_lines > 0:
            progress = (current_count / total_lines * 100)
            print(f"  ⚡ {current_count:>10,} / {total_lines:,} edges ({progress:>5.1f}%) | "
                  f"Current: {current_rate:>8,.0f} edges/sec | "
                  f"Average: {avg_rate:>8,.0f} edges/sec")
        else:
            print(f"  ⚡ {current_count:>10,} edges | "
                  f"Current: {current_rate:>8,.0f} edges/sec | "
                  f"Average: {avg_rate:>8,.0f} edges/sec")
        
        last_count = current_count
        last_time = current_time
    
    # Wait for all processes
    for w in workers:
        w.join()
    
    # Send -1 to EVERY Kafka partition so every consumer thread sees it
    # and drains its own slot before stopping.
    send_end_of_stream_to_all_partitions(bootstrap_server, topic)

    # Collect errors
    errors = []
    while not error_queue.empty():
        try:
            errors.append(error_queue.get_nowait())
        except Empty:
            break
    
    total_time = time.time() - start_time
    final_count = counter.value
    avg_rate = final_count / total_time
    
    print()
    print(f"╔{'═'*70}╗")
    print(f"║ {'PARALLEL STREAMING COMPLETED':^68} ║")
    print(f"╠{'═'*70}╣")
    print(f"║ Total edges:    {final_count:>10,} edges{' '*38} ║")
    print(f"║ Time taken:     {total_time:>10.1f} seconds{' '*36} ║")
    print(f"║ Average rate:   {avg_rate:>10,.0f} edges/sec{' '*32} ║")
    print(f"║ Workers used:   {num_workers:>10}{' '*48} ║")
    
    if errors:
        print(f"║ Errors:         {len(errors):>10}{' '*48} ║")
    
    # Estimates
    time_1m = 1000000 / avg_rate
    time_10m = 10000000 / avg_rate
    time_100m = 100000000 / avg_rate
    time_1b = 1000000000 / avg_rate
    
    print(f"╠{'═'*70}╣")
    print(f"║ {'Time Estimates':<68} ║")
    print(f"║   1M edges:     {time_1m:>10.1f} seconds = {time_1m/60:>6.1f} min{' '*23} ║")
    print(f"║   10M edges:    {time_10m:>10.1f} seconds = {time_10m/60:>6.1f} min{' '*23} ║")
    print(f"║   100M edges:   {time_100m:>10.1f} seconds = {time_100m/3600:>6.1f} hours{' '*21} ║")
    print(f"║   1B edges:     {time_1b:>10.1f} seconds = {time_1b/3600:>6.1f} hours{' '*21} ║")
    print(f"╚{'═'*70}╝")
    
    if errors[:5]:  # Show first 5 errors
        print("\n⚠️  Sample errors:")
        for err in errors[:5]:
            print(f"    {err}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    
    file_path = sys.argv[1]
    topic = sys.argv[2]
    mode = sys.argv[3].upper() if len(sys.argv) > 3 else 'BALANCED'
    num_workers = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    bootstrap_server = "172.28.5.8:9092"
    input_format_arg = 'auto'

    if len(sys.argv) > 5:
        arg5 = sys.argv[5].lower()
        if arg5 in ('auto', 'json', 'jsonl', 'csv'):
            input_format_arg = arg5
        else:
            bootstrap_server = sys.argv[5]

    if len(sys.argv) > 6:
        input_format_arg = sys.argv[6].lower()

    input_format = resolve_input_format(file_path, input_format_arg)
    
    # Select configuration
    if mode == 'FAST':
        config = PerformanceMode.FAST
    elif mode == 'EXTREME':
        config = PerformanceMode.EXTREME
    else:
        config = PerformanceMode.BALANCED
    
    # Validate file
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)

    # Stream edges
    if num_workers > 1:
        stream_edges_parallel(file_path, topic, bootstrap_server, config, num_workers, input_format)
    else:
        stream_edges_single(file_path, topic, bootstrap_server, config, input_format)
