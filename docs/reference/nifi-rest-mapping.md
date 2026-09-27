---
title: NiFi 2 REST paths used by nifi-mcp
type: reference
status: active
tags: [nifi, rest]
updated: 2026-09-27
---

# REST mapping

Source: `apache/nifi` `nifi-web-api` (main). All paths are under `/nifi-api`.

| Client method | Method | Path | Resource class |
|---|---|---|---|
| `about` | GET | `/flow/about` | FlowResource |
| `current_user` | GET | `/flow/current-user` | FlowResource |
| `get_flow` | GET | `/flow/process-groups/{id}` | FlowResource |
| `search` | GET | `/flow/search-results?q=` | FlowResource |
| `list_processor_types` | GET | `/flow/processor-types` | FlowResource |
| `get_processor_definition` | GET | `/flow/processor-definition/{g}/{a}/{v}/{type}` | FlowResource |
| `schedule_process_group` | PUT | `/flow/process-groups/{id}` | FlowResource |
| `enable_controller_services_in_group` | PUT | `/flow/process-groups/{id}/controller-services` | FlowResource |
| `bulletins` | GET | `/flow/bulletin-board?after=<bulletin id>` | FlowResource (`after` is a bulletin id cursor, exposed as `after_id`) |
| `list_parameter_contexts` | GET | `/flow/parameter-contexts` | FlowResource |
| `get_parameter_context` | GET | `/parameter-contexts/{id}` | ParameterContextResource |
| `create_parameter_context` | POST | `/parameter-contexts` | ParameterContextResource |
| `update_parameter_context` | POST, GET, DELETE | `/parameter-contexts/{id}/update-requests[/{requestId}]` | ParameterContextResource |
| `delete_parameter_context` | DELETE | `/parameter-contexts/{id}?version=` | ParameterContextResource |
| `set_process_group_parameter_context` | PUT | `/process-groups/{id}` with `component.parameterContext.id`, plus `processGroupUpdateStrategy: ALL_DESCENDANTS` when recursive | ProcessGroupResource |
| `create_process_group` | POST | `/process-groups/{id}/process-groups` | ProcessGroupResource |
| `create_processor` | POST | `/process-groups/{id}/processors` | ProcessGroupResource |
| `create_connection` | POST | `/process-groups/{id}/connections` | ProcessGroupResource |
| `create_controller_service` | POST | `/process-groups/{id}/controller-services` | ProcessGroupResource |
| `create_port` | POST | `/process-groups/{id}/input-ports` or `output-ports` | ProcessGroupResource |
| `get_port` / `update_port` / `delete_port` | GET/PUT/DELETE | `/input-ports/{id}` or `/output-ports/{id}` (DELETE takes `?version=&clientId=`) | InputPortResource / OutputPortResource |
| `download_flow` | GET | `/process-groups/{id}/download` | ProcessGroupResource |
| `import_flow` | POST | `/process-groups/{id}/process-groups/import` | ProcessGroupResource |
| `replace_flow` | POST | `/process-groups/{id}/replace-requests` | ProcessGroupResource |
| `get_processor` / update / delete | GET/PUT/DELETE | `/processors/{id}` | ProcessorResource |
| `set_processor_run_status` | PUT | `/processors/{id}/run-status` | ProcessorResource |
| `get_connection` / `update_connection` / delete | GET/PUT/DELETE | `/connections/{id}` (queue settings: `backPressureObjectThreshold`, `backPressureDataSizeThreshold`, `flowFileExpiration`) | ConnectionResource |
| `list_queue` | POST | `/flowfile-queues/{id}/listing-requests` | FlowFileQueueResource |
| `empty_queue` | POST | `/flowfile-queues/{id}/drop-requests` | FlowFileQueueResource |
| controller service CRUD / run-status | GET/PUT/DELETE | `/controller-services/{id}` | ControllerServiceResource |
| JWT mint | POST | `/access/token` | AccessResource |

Creates require `revision.version = 0`. Updates send the current version. Cluster writes send `disconnectedNodeAcknowledged`.

Parameter context updates go through the async update-request, not `PUT /parameter-contexts/{id}`: the PUT
refuses while any referencing component is running, the update-request stops and restarts them itself.
A parameter entry that carries only `name` deletes that parameter (`StandardParameterContextDAO`).
