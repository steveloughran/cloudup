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

TODO

* Retries on failed uploads
* Save text & avro summaries of uploads (src, dest, size, start time, end time, etag )
* Drive off text file rather than list files. 
* Parallel tree walk.
* Patterns to upload.


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

Ignore upload errors until the end of the operation.

