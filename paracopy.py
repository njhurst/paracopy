#!/usr/bin/env python3
"""
This script is used to copy files from a source directory to a destination directory using multiple worker threads.
It provides options for verbose output, dry run (no actual file copying), and deleting source files after copying.
The progress of the file copying process is displayed, including the number of files copied, skipped, and the total bytes copied.

It was written to work with high latency network file systems, such as Google Drive File Stream, where copying a 
large number of files, especially small files, can be very slow.  It is designed to be run on a single machine,

The script uses a producer-consumer model, where the producer thread walks the source directory and adds files to 
a queue, and the consumer threads copy files from the queue to the destination directory.  The number of consumer
threads is specified by the --max_workers option.  The default is 100.

The script uses a dictionary to keep track of files currently being copied, indexed by source path, with (dest_path, bytes_copied, bytes_total) as value.
The bytes_copied and bytes_total values are used to display the progress of the file copy operation.

The script also uses a dictionary to cache the destination directories that have been created.  This is used to avoid
repeatedly calling os.makedirs() for the same directory.

The script uses a monitor thread to display the progress of the file copying process.  The monitor thread displays
the most recent file being copied, the number of files copied, skipped, and the total bytes copied.  It also displays
the number of active files being copied, the number of files in the queue, and the percentage of files copied.

The script uses a stats thread to count the number of files to copy in the background.  This is used to display the
total number of files to copy, and the total size of the files to copy.  This is slow, so it is done in the background.

The files are copied incrementally, 2MB at a time.  This is done as the target file system, seaweedfs, stores
files in 2MB chunks.  This allows the script to copy files in parallel and avoid creating tail pieces, and to 
resume copying if the script is interrupted.  The script also uses the file size and modification time to determine 
if a file needs to be copied.

Usage:
python paracopy.py source_directory destination_directory [--max_workers MAX_WORKERS] [--verbose] [--dry_run] [--delete_source]

Arguments:
source_directory         Path to the source directory
destination_directory    Path to the destination directory

Options:
--max_workers MAX_WORKERS    Maximum number of worker threads (default: 100)
--verbose                    Print verbose output
--dry_run                    Dry run (do not copy files)
--delete_source              Delete source files after copying
"""


import os
import shutil
import concurrent.futures
import time
import sys
import queue
import threading
import traceback
import argparse

total_files_copied = 0
total_bytes_copied = 0
total_files_skipped = 0
prev_bytes_copied = 0

# a dictionary of files currently being copied, indexed by source path, with (dest_path, bytes_copied, bytes_total) as value

active_files_lock = threading.Lock()
active_files = {}
finished = False
most_recent_file = None
total_files_to_copy = None
total_size = None

# options
verbose = False
dry_run = False
delete_source = False

# cache dest dirs that have been created
dest_dirs = {}

def pretty_size(size):
    """Return a human readable size string."""
    if size < 1024:
        return f"{size}B"
    elif size < 1024 * 1024:
        return f"{size/1024:.2f}KB"
    elif size < 1024 * 1024 * 1024:
        return f"{size/1024/1024:.2f}MB"
    elif size < 1024 * 1024 * 1024 * 1024:
        return f"{size/1024/1024/1024:.2f}GB"
    else:
        return f"{size/1024/1024/1024/1024:.2f}TB"

def monitor(file_queue):
    """Print progress every 2 seconds."""
    global finished
    global prev_bytes_copied
    global total_files_skipped
    global total_files_copied
    global total_bytes_copied
    global active_files
    global total_files_to_copy

    prev_time = time.time()
    prev_files_copied = 0
    while not finished:
        time.sleep(2)
        copy_rate = (total_bytes_copied - prev_bytes_copied) / (time.time() - prev_time) / 1024 / 1024
        files_per_second = (total_files_copied - prev_files_copied) / (time.time() - prev_time)
        prev_files_copied = total_files_copied
        file_progress = 0
        if total_files_to_copy:
            file_progress = (total_files_copied + total_files_skipped) / total_files_to_copy * 100
        # lock the active_files dictionary while printing
        with active_files_lock:
            active_files_list = list(active_files.items()).copy()
        active_files_list.sort(key=lambda x: x[1][1]/x[1][2] if x[1][2] > 0 else 0)
        for source_path, (dest_path, bytes_copied, bytes_total) in active_files_list:
            if bytes_copied > 0 and bytes_total > 0:
                filename = os.path.basename(source_path)
                print(f"  {filename} -> {pretty_size(bytes_copied)} / {pretty_size(bytes_total)} ({bytes_copied / bytes_total * 100:.2f}%)")
        print(f"Most recent file: {most_recent_file}")
        print(f"Copied {total_files_copied}+{total_files_skipped} = {total_files_copied+total_files_skipped}/{total_files_to_copy} files ({pretty_size(total_bytes_copied)} bytes) "
              f"skipped {total_files_skipped} {copy_rate:.3f}MB/s {files_per_second:.3f} fps "
              f"{len(active_files)} active files {file_queue.qsize()} in queue {file_progress:.2f}% complete")
        prev_time = time.time()
        prev_bytes_copied = total_bytes_copied


def copy_file(source_path, dest_path):
    """Copy a single file from source_path to dest_path."""
    global total_files_skipped
    global most_recent_file
    global total_files_copied, total_bytes_copied
    most_recent_file = source_path
    if dry_run:
        return

    # handle special files
    if os.path.islink(source_path):
        if os.path.exists(dest_path):
            os.remove(dest_path)
        os.symlink(os.readlink(source_path), dest_path)
        total_files_copied += 1
        return


    if (os.path.exists(dest_path) and 
        os.path.getsize(source_path) == os.path.getsize(dest_path) and 
        os.path.getmtime(source_path) <= os.path.getmtime(dest_path)):
        total_files_skipped += 1
    else:
        if not os.path.exists(dest_path) or os.path.getsize(source_path) > os.path.getsize(dest_path):
            # incrementally copy
            start = 0
            if os.path.exists(dest_path):
                start = os.path.getsize(dest_path)
            # align to 2MB boundary
            start = start - (start % 2097152)
            final_size = os.path.getsize(source_path)
            with active_files_lock:
                active_files[source_path] = (dest_path, start, final_size)
            with open(source_path, 'rb') as source_file:
                source_file.seek(start)
                with open(dest_path, 'wb') as dest_file:
                    dest_file.seek(start)
                    # read 2MB block at a time
                    while True:
                        data = source_file.read(2097152)
                        if not data:
                            break
                        dest_file.write(data)
                        total_bytes_copied += len(data)
                        start += len(data)
                        with active_files_lock:
                            active_files[source_path] = (dest_path, start, final_size)
            with active_files_lock:
                del active_files[source_path]

        shutil.copystat(source_path, dest_path)
        total_files_copied += 1
    # delete source file
    if delete_source:
        os.remove(source_path) 



def file_producer(source_dir, file_queue):
    """Producer function to add files to the queue."""
    global most_recent_file
    for root, _, files in os.walk(source_dir):
        for file in files:
            file_path = os.path.join(root, file)
            # print('.', end='', flush=True)
            file_queue.put(file_path)
            most_recent_file = file_path
    file_queue.put(None)  # Sentinel to signal the end

def file_consumer(source_dir, dest_dir, file_queue):
    """Consumer function to copy files from the queue."""
    while True:
        file_path = file_queue.get()
        # print('^', end='', flush=True)
        if file_path is None:  # Check for the sentinel
            file_queue.put(None)  # Put the sentinel back for other threads
            break
        try:
            dest_path = os.path.join(dest_dir, os.path.relpath(file_path, source_dir))
            target_dir_name = os.path.dirname(dest_path)
            if target_dir_name not in dest_dirs:
                os.makedirs(target_dir_name, exist_ok=True)
                dest_dirs[target_dir_name] = True
            copy_file(file_path, dest_path)
        except FileNotFoundError:
            print(f"File {file_path} not found")
        except PermissionError:
            print(f"Permission error on file {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            print(e)
            # print stack trace
            traceback.print_exc()
        with active_files_lock:
            if file_path in active_files:
                del active_files[file_path]

        file_queue.task_done()

def count_files(source_dir):
    """Count the number of files in source_dir."""
    count = 0
    for root, _, files in os.walk(source_dir):
        count += len(files)
    global total_files_to_copy
    total_files_to_copy = count
    # measure total size.  This turns out to be too slow
    # global total_size
    # total_size = 0
    # for root, _, files in os.walk(source_dir):
    #     for file in files:
    #         file_path = os.path.join(root, file)
    #         total_size += os.path.getsize(file_path)

    return count

def main(source_dir, dest_dir, max_workers):
    """Copy files from source_dir to dest_dir using concurrent workers."""
    file_queue = queue.Queue(maxsize=max_workers * 2)  # Buffer size
    producer_thread = threading.Thread(target=file_producer, args=(source_dir, file_queue))
    producer_thread.start()

    monitor_thread = threading.Thread(target=monitor, args=(file_queue,))
    monitor_thread.start()

    # count the number of files to copy in background
    stats_thread = threading.Thread(target=count_files, args=(source_dir,))
    stats_thread.start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(file_consumer, source_dir, dest_dir, file_queue) for _ in range(max_workers)]
        concurrent.futures.wait(futures)

    start_time = time.time()  # Define the variable start_time
    producer_thread.join()

    global finished
    finished = True

    # wait for the queue to empty
    file_queue.join()

    # print final stats
    print(f"Total files copied: {total_files_copied}")
    print(f"Total files skipped: {total_files_skipped}")
    print(f"Total bytes copied: {pretty_size(total_bytes_copied)}")
    print(f"Total size: {pretty_size(total_size)}")
    print(f"Total time: {time.time() - start_time:.2f}s")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy files from source directory to destination directory.')
    parser.add_argument('source_directory', type=str, help='Path to the source directory')
    parser.add_argument('destination_directory', type=str, help='Path to the destination directory')
    parser.add_argument('--max_workers', type=int, default=100, help='Maximum number of worker threads')
    parser.add_argument('--verbose', action='store_true', help='Print verbose output')
    parser.add_argument('--dry_run', action='store_true', help='Dry run (do not copy files)')
    parser.add_argument('--delete_source', action='store_true', help='Delete source files after copying')
    args = parser.parse_args()
    verbose = args.verbose
    dry_run = args.dry_run
    delete_source = args.delete_source
    if not os.path.exists(args.source_directory):
        print(f"Source directory {args.source_directory} does not exist")
        sys.exit(1)
    if not os.path.exists(args.destination_directory):
        print(f"Destination directory {args.destination_directory} does not exist")
        sys.exit(1)

    main(args.source_directory, args.destination_directory, args.max_workers)
