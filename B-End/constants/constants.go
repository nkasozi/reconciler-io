package constants

import "time"

var FILE_SECTION_BATCH_SIZE = 100
var NATS_URL = "nats://localhost:4222"
var PRIMARY_FILE_SECTIONS_STREAM_NAME = "primary-file-sections-stream"
var COMPARISON_FILE_SECTIONS_STREAM_NAME = "comparison-file-sections-stream"
var FILE_RECONSTRUCTION_STREAM_NAME = "file-sections-to-be-reconstructed-stream"
var DEFAULT_NATS_TIMEOUT_IN_MINUTES = time.Duration(2 * time.Minute)
