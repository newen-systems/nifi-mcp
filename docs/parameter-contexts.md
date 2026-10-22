# Parameter ownership and context composition

Every new parameter uses `root.service.key[.key]`. The first segment is also the
exact name of its owning parameter context. Segments are lowercase, start with a
letter, and contain letters, digits or hyphen-separated words. Use dots for the
hierarchy; do not flatten names with underscores or invent aliases for each flow.

| Owning context | Parameter | Processor property reference |
| --- | --- | --- |
| gitlab | gitlab.pypi.username | #{gitlab.pypi.username} |
| gitlab | gitlab.pypi.password | #{gitlab.pypi.password} |
| gitlab | gitlab.container.registry.url | #{gitlab.container.registry.url} |
| redis | redis.cache.host | #{redis.cache.host} |
| s3 | s3.artifacts.bucket.name | #{s3.artifacts.bucket.name} |

The root identifies the configuration owner/provider, the service identifies its
capability, and the remaining segments identify the setting. Reuse the same name
for the same setting across flows. Keep genuinely different services distinct.
Discover the existing context before creating another. Names are case-sensitive;
MCP rejects malformed new names and parameters whose root differs from their owning
context. It never silently normalises a name, moves a value, or rewrites a reference.

Create a root context using `nifi_create_parameter_context`:

```json
{
  "params": {
    "cluster": "nifi-dev",
    "name": "gitlab",
    "parameters": [
      {"name": "gitlab.pypi.username", "value": "package-reader"}
    ]
  }
}
```

Credentials such as passwords and tokens must explicitly set `sensitive: true`.
Supply their values through the approved secret workflow. A name is not a secret
store and does not grant NiFi access permissions.

NiFi binds one context to each process group. For a flow requiring several roots,
create a composed context with `inherited_context_ids` pointing to the existing root
contexts and bind that composed context. For example, an `image-transfer` context
can inherit `gitlab` and `s3`; it does not own copies of `gitlab.*` or `s3.*` values.
The values remain discoverable in their root contexts. This is parameter-context
inheritance, separate from applying a binding recursively to child process groups.

On create, `inherited_context_ids` is an optional list of context IDs. On update,
omitting it preserves inheritance, a list replaces inheritance, and `[]` clears it.
NiFi validates inheritance and access policies. Context detail output includes
`inherited_contexts` and sorts parameters lexically so roots and services appear
together. A root context can own its parameters; a composed context should inherit
shared settings rather than duplicating them. Resolve any overlapping inherited
names explicitly before binding; do not rely on accidental shadowing.

Existing parameters remain readable, editable and removable under their original
names. New entries must follow the convention. Renaming a context cannot invalidate
the namespace of its remaining dot-named owned parameters. Removing and adding a
parameter is not an atomic rename of processor references. A legacy migration must
inventory references, preserve sensitive flags and values through the secret store,
plan component lifecycle changes, update references and verify resolution before
removing old entries. This implementation performs no live migration.

The inheritance request uses NiFi's reference entities with both `id` and
`component.id`. See the [NiFi 2.12 parameter context DTO](https://github.com/apache/nifi/blob/rel/nifi-2.12.0/nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/dto/ParameterContextDTO.java)
and [DAO inheritance handling](https://github.com/apache/nifi/blob/rel/nifi-2.12.0/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/main/java/org/apache/nifi/web/dao/impl/StandardParameterContextDAO.java).
