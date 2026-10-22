# Inspect a connection queue

Call `nifi_list_queue` with the connection ID and the exact cluster name:

```json
{
  "params": {
    "cluster": "nifi-dev",
    "connection_id": "11111111-1111-1111-1111-111111111111",
    "limit": 10,
    "timeout_seconds": 15,
    "include_attributes": true
  }
}
```

The tool creates a temporary NiFi listing request, polls until complete, and deletes
that request even when polling fails. It never consumes, drops or rearranges queued
FlowFiles and works in MCP read-only mode. NiFi still requires read-source-data
permission for the connection.

`listingRequest.flowFileSummaries` contains NiFi's sampled FlowFiles, including UUID,
filename, size and node information when available. `returnedCount` is the number
returned; `sampleTruncated` indicates that the requested limit clipped NiFi's sample.
Even when false, it does not mean the entire queue was listed. This API is a sample,
not pagination. The default limit is 100, with a maximum of 10000. The timeout applies
to listing polling, not subsequent per-FlowFile metadata requests.

With `include_attributes: true`, each summary gains `details`, containing the
FlowFile metadata and attributes. Requests target the `clusterNodeId` from that
summary. Payload bytes are not downloaded. Metadata reads are sequential and bounded
by the sample limit; use a small limit for attribute inspection. Normal response
redaction applies.

FlowFiles can leave the queue between listing and inspection; a missing item causes
a read error. Stop the relevant processors separately if a stable diagnostic sample
is required. Listing itself never stops processors.

REST contract: Apache NiFi 2.12 FlowFileQueueResource, POST/GET/DELETE listing requests
and GET `/flowfile-queues/{id}/flowfiles/{uuid}?clusterNodeId={node}`.
