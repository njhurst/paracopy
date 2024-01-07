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
