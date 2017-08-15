    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License. See accompanying LICENSE file.


# cloudup

High performance upload of data to cloud storage via Apache Hadoop.

This program is the equivalent to `hdfs put`, but specifically targeting object stores
as the destination.

* Parallel execution of uploads.
* Sort of initial dataset to identify largest few files and commence
upload immediately.
* Shuffled selection of the next files to upload, to avoid throttled
hotsposts.
* Printing summary statistics, including any provided by
the filesystem client.

### TODO

* Retries on failed uploads
* Save text & avro summaries of uploads (src, dest, size, start time, end time, etag )
* Drive off text file rather than list files. 
* Parallel tree walk to accelerate initial listing.
* Patterns to select upload files.


## Usage

```
hadoop org.apache.hadoop.tools.cloudup.Cloudup -s <source> -d <dest> -t <threads> [-l <largest>] [-i]
```


### Source `-s <source>` or `--source <source>`

Source path in the local filesystem. A `file://` path may be used, or
a local FS path.


### Dest `-d <dest>` or `--dest <dest>`

Destination path. This must be an absolute filesystem URI.

The local filesystem may be used as a destination -in which case the destination
path must not be under the source path (or vice-versa).

### Threads `-t <threads>` or `--threads <threads>`

Number of threads to use for uploading

**Important**: increasing the number of threads does not guarantee higher
upload performance. You are constrained by the bandwidth of the local disk,
and the uplink to the remote filesystem.

If the remote object store throttles requests to specific shards, or implicit
calls to other services (authentication, encryption and the like), then
high thread numbers may actually slow down the upload.


### Larges files `-l <largest>` or `--largest <largest>`

Number of large files to uplaod immediately, before picking files to
upload at random.

### Ignore errors `-i`

Ignore upload errors.

There's a pre-launch check that the destination path is reachable; this
error is always thrown, as it is generally a sign that there are configuration,
connectivity, or authentication problems.  

### Overwrite `-o`

Overwrite existing files. If unset, any attempt to overwrite an existing
file will trigger an error.

## Storage-Specific features

Cloudup is designed to work with any destination filesystem supported
via the Hadoop APIs, though it is written with Amazon S3 and Microsoft Azure
WASB storage in mind. It uses the API call `FileSystem.copyFromLocalFile()`,
which defaults to reading the source file into a buffer, then uploading
these buffers through a stream created with `FileSystem.create()`.
If the FileSystem implementation subclass implements a custom
`copyFromLocalFile()` operation, it can deliver higher performance.

### S3A

The S3A FileSystem client uses the AWS SDK's transfer
manager to directly upload the source files. As a result

1. The transfer manager's threading and HTTP connection re-use mechanisms
are those provided by the AWS SDK; this includes an unbounded thread pool
for parallel upload of different parts in a multipart upload.

1. The thread limit set in `fs.s3a.threads.max` is only used to set the
limit on other parallelised operations performed by the S3A connector,
*not the upload of source data itself*

## Examples

Copy the parent directory & children to `s3a://hwdev-steve-ireland-3w/copy`,
overwriting destination files. 256 S3A threads (ignoring the S3A transfer
manager). Use 128 workers for simultaneous upload of up to 256 files, and
sort the source input to identify the 64 largest files for upload ahead
of any shuffle phase

```bash
hadoop  jar cloudup-2.8.jar org.apache.hadoop.tools.cloudup.Cloudup  -D fs.s3a.threads.max=256 -s ..  \
  -d s3a://hwdev-steve-ireland-3w/copy -t 128 -l 64 -o

```

