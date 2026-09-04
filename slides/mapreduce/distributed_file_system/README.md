# Distributed File System

**What is a DFS?** A distributed file system (DFS) lets programs on many
machines store and access files over a network as if those files were
local, spreading data (and its replicas) across a cluster instead of one
disk.

Background reading on the storage layer beneath MapReduce/Hadoop: a survey
of distributed file systems (culminating in HDFS) and Google's Bigtable
paper, the structured-storage system built on top of Google's own
distributed file system (GFS).

## Contents

| Name | Type | Description |
|---|---|---|
| [`Distributed_File_Systems_slides.pdf`](Distributed_File_Systems_slides.pdf) | pdf (884.6KB) | Seminar slides surveying distributed file systems — FTP, NFS, AFS (with a performance comparison), Google File System (GFS), Amazon Dynamo, and HDFS (Prabhakaran Murugesan, ECE 658, Colorado State University) |
| [`Bigtable_A_Distributed_Storage_System_for_Structured_Data.pdf`](Bigtable_A_Distributed_Storage_System_for_Structured_Data.pdf) | pdf (220.6KB) | Google's original Bigtable paper (Chang et al., OSDI 2006) — the distributed storage system for structured data built on top of GFS |
