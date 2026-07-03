local jsonpath = import 'github.com/jsonnet-libs/xtd/jsonpath.libsonnet';

{
  _config+:: {
    // Whether the compartments architecture is enabled.
    compartments_enabled: false,

    // Number of read and write compartments.
    compartments_read_count: 1,
    compartments_write_count: 1,

    // Kafka topic produced/consumed per read compartment. Must contain the '<read-compartment-id>'
    // placeholder, which Mimir replaces with the read compartment index at runtime.
    compartments_ingest_storage_kafka_topic: 'ingest-rc-<read-compartment-id>',

    // Kafka cluster address template for compartment components. Carries the '<write-compartment-id>'
    // placeholder, which Mimir replaces with the write compartment index at runtime (distributors target
    // their own cluster; ingesters consume from every write compartment's cluster). Mimir's jsonnet does
    // not deploy Kafka, so this is a fictitious address mirroring the non-compartment default.
    compartments_ingest_storage_kafka_address: 'kafka-wc-<write-compartment-id>.%(namespace)s.svc.%(cluster_domain)s:9092' % $._config,

    // Blocks-storage bucket per read compartment. Must contain the '<read-compartment-id>' placeholder,
    // which the querier replaces with each read compartment index to read that compartment's bucket (each
    // read compartment's store-gateways and compactors own a dedicated bucket).
    compartments_blocks_storage_bucket_name: '%s-rc-<read-compartment-id>' % $._config.blocks_storage_bucket_name,
  },

  assert $._config.compartments_read_count >= 1 : 'compartments_read_count must be >= 1',
  assert $._config.compartments_write_count >= 1 : 'compartments_write_count must be >= 1',

  assert !$._config.compartments_enabled || $._config.ruler_remote_evaluation_enabled
         : 'compartments require ruler remote rule evaluation (ruler_remote_evaluation_enabled)',

  assert !$._config.compartments_enabled || std.length(std.findSubstr('<read-compartment-id>', $._config.compartments_blocks_storage_bucket_name)) > 0
         : 'compartments_blocks_storage_bucket_name must contain the "<read-compartment-id>" placeholder',

  // Concrete blocks-storage bucket name for the given read compartment, resolving the
  // '<read-compartment-id>' placeholder in compartments_blocks_storage_bucket_name.
  mimirBlocksStorageCompartmentBucketName(compartmentID):: std.strReplace($._config.compartments_blocks_storage_bucket_name, '<read-compartment-id>', std.toString(compartmentID)),

  // Concrete Kafka topic for the given read compartment, resolving the '<read-compartment-id>' placeholder
  // in compartments_ingest_storage_kafka_topic.
  mimirIngestStorageCompartmentKafkaTopic(compartmentID):: std.strReplace($._config.compartments_ingest_storage_kafka_topic, '<read-compartment-id>', std.toString(compartmentID)),

  // The blocks-storage bucket-name CLI flag for the configured storage backend.
  mimirBlocksStorageBucketNameFlag::
    if $._config.storage_backend == 'gcs' then 'blocks-storage.gcs.bucket-name'
    else if $._config.storage_backend == 's3' then 'blocks-storage.s3.bucket-name'
    else if $._config.storage_backend == 'azure' then 'blocks-storage.azure.container-name'
    else error 'compartments do not support the "%s" storage backend' % $._config.storage_backend,

  // Root CLI args shared by every compartment-aware Mimir component.
  mimirCompartmentsRootArgs:: {
    'compartments.enabled': true,
    'compartments.read.num-compartments': $._config.compartments_read_count,
    'compartments.write.num-compartments': $._config.compartments_write_count,
  },

  // Common CLI args shared by compartment-aware Mimir components that consume or produce ingest-storage data.
  mimirCompartmentsCommonArgs:: $.mimirCompartmentsRootArgs {
    'ingest-storage.kafka.topic': $._config.compartments_ingest_storage_kafka_topic,
  },

  // Apply a mixin to every compartment entry in a compartments resource map.
  // Pass super.fieldName as compartmentMap to match the established super-in-for-clause convention.
  // mixin can be either a static object applied to all compartments, or a function(compartmentIdx)
  // that returns a per-compartment mixin.
  // Example: foo_zone_a_deployments+: $.mimirCompartmentsOverrides(super.foo_zone_a_deployments, someMixin),
  // Example: foo_zone_a_compartments_args+: $.mimirCompartmentsOverrides(super.foo_zone_a_compartments_args, function(compartmentIdx) { 'compartment-id': compartmentIdx }),
  mimirCompartmentsOverrides(compartmentMap, mixin):: {
    [compartmentKey]+: if std.isFunction(mixin) then mixin(std.parseInt(std.split(compartmentKey, '_')[1])) else mixin
    for compartmentKey in std.objectFields(compartmentMap)
  },

  // Creates a { compartment_0: …, compartment_1: … } resource map when enabled, otherwise returns {}.
  // fn is called with the compartment index (integer) for each compartment in [0, numCompartments).
  // Usage: foo_zone_a_deployments: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments,
  //          function(compartment) $.newFooCompartmentDeployment('a', compartment, …)),
  mimirCompartmentsCreateIf(enabled, numCompartments, fn):: if !enabled then {} else {
    ['compartment_%d' % compartment]: fn(compartment)
    for compartment in std.range(0, numCompartments - 1)
  },

  // Base (empty) per-compartment caching config.
  blocks_chunks_zone_a_caching_configs:: $.mimirCompartmentsCreateIf(true, $._config.compartments_read_count, function(c) {}),
  blocks_chunks_zone_b_caching_configs:: $.mimirCompartmentsCreateIf(true, $._config.compartments_read_count, function(c) {}),
  blocks_chunks_zone_c_caching_configs:: $.mimirCompartmentsCreateIf(true, $._config.compartments_read_count, function(c) {}),

  validateMimirCompartmentsDistributorZones()::
    if !$._config.compartments_distributor_enabled || std.length($._config.multi_zone_availability_zones) >= 1 then
      null
    else
      'compartments_distributor_enabled requires at least one configured multi_zone_availability_zones entry',

  // Validates that per-compartment Deployments/StatefulSets configuration options are correctly
  // parameterised for the compartment they belong to.
  validateMimirCompartmentsConfig(resourceNames, requireCommonFlags=false)::
    local root = $;
    local targetFlag = '-target';
    local rootFlags = [
      { flag: '-compartments.enabled', value: std.toString(root.mimirCompartmentsRootArgs['compartments.enabled']) },
      { flag: '-compartments.read.num-compartments', value: std.toString(root.mimirCompartmentsRootArgs['compartments.read.num-compartments']) },
      { flag: '-compartments.write.num-compartments', value: std.toString(root.mimirCompartmentsRootArgs['compartments.write.num-compartments']) },
    ];
    local addressFlag = '-ingest-storage.kafka.address';
    local topicFlag = '-ingest-storage.kafka.topic';
    local blocksBucketFlag = '-' + root.mimirBlocksStorageBucketNameFlag;
    local rulerQueryFrontendAddressFlag = '-ruler.query-frontend.address';
    local writeIdSuffix = '.write-compartment-id';
    local readIdSuffix = '.read-compartment-id';
    local writePlaceholder = '<write-compartment-id>';
    local readPlaceholder = '<read-compartment-id>';

    local isDigit(c) = std.length(std.findSubstr(c, '0123456789')) > 0;

    // Whether s contains token not immediately followed by a digit, so "-wc-1" doesn't match "-wc-10".
    local containsIdToken(s, token) =
      std.foldl(
        function(found, idx)
          found || (local after = idx + std.length(token); after >= std.length(s) || !isDigit(s[after])),
        std.findSubstr(token, s),
        false
      );

    // Integer id that follows the last occurrence of marker in name.
    local idAfterMarker(name, marker) =
      local indices = std.findSubstr(marker, name);
      local start = indices[std.length(indices) - 1] + std.length(marker);
      std.parseInt(std.substr(name, start, std.length(name) - start));

    // {kind: 'write'|'read', id: int} extracted from a resource name, or {kind: 'global'} when the resource
    // belongs to no compartment (e.g. the query-frontend or querier, which span the whole topology).
    local extractCompartment(name) =
      if std.length(std.findSubstr('-wc-', name)) > 0 then
        { kind: 'write', id: idAfterMarker(name, '-wc-') }
      else if std.length(std.findSubstr('-rc-', name)) > 0 then
        { kind: 'read', id: idAfterMarker(name, '-rc-') }
      else
        { kind: 'global' };

    // Value of a "-flag=value" CLI arg in a container, or null if absent.
    local flagValue(container, flag) =
      if std.objectHas(container, 'args') then
        local prefix = flag + '=';
        local matching = std.filter(function(arg) std.isString(arg) && std.startsWith(arg, prefix), container.args);
        if std.length(matching) == 0 then null else std.substr(matching[0], std.length(prefix), std.length(matching[0]))
      else
        null;

    // First "-flag=value" CLI arg whose flag name ends with suffix, as {flag, value}, or null if absent.
    local flagBySuffix(container, suffix) =
      if !std.objectHas(container, 'args') then
        null
      else
        local matching = std.filter(
          function(arg)
            std.isString(arg) && std.startsWith(arg, '-') &&
            (local eq = std.findSubstr('=', arg);
             std.length(eq) > 0 && std.endsWith(std.substr(arg, 0, eq[0]), suffix)),
          container.args
        );
        if std.length(matching) == 0 then
          null
        else
          local arg = matching[0];
          local eq = std.findSubstr('=', arg)[0];
          { flag: std.substr(arg, 0, eq), value: std.substr(arg, eq + 1, std.length(arg) - eq - 1) };

    local hasTarget(container, target) =
      local value = flagValue(container, targetFlag);
      value != null && std.member(std.split(value, ','), target);

    local usesCompartmentKafkaAddress(container) =
      local kafkaAddressTargets = [
        'distributor',
        'ingester',
        'query-frontend',
        'ruler',
        'ruler-query-frontend',
        'compactor',
        'compactor-scheduler',
        'store-gateway',
      ];
      std.foldl(function(found, target) found || hasTarget(container, target), kafkaAddressTargets, false);

    local validateKafkaAddress(name, compartment, container) =
      local value = flagValue(container, addressFlag);
      if value == null then
        null
      else if !usesCompartmentKafkaAddress(container) then
        null
      else if compartment.kind == 'write' then
        if std.length(std.findSubstr(writePlaceholder, value)) > 0 then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but write compartment %d must target its own Kafka cluster (the address must contain "-wc-%d", not the "%s" placeholder).' % [name, addressFlag, value, compartment.id, compartment.id, writePlaceholder]
        else if !containsIdToken(value, '-wc-%d' % compartment.id) then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but write compartment %d must target its own Kafka cluster (the address must contain "-wc-%d").' % [name, addressFlag, value, compartment.id, compartment.id]
        else
          null
      else if std.length(std.findSubstr(writePlaceholder, value)) == 0 then
        if compartment.kind == 'read' then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but a read compartment consumes from every write compartment\'s Kafka cluster, so the address must contain the "%s" placeholder.' % [name, addressFlag, value, writePlaceholder]
        else
          'The Deployment or StatefulSet "%s" sets "%s=%s", but a global deployment reads from every write compartment\'s Kafka cluster, so the address must contain the "%s" placeholder.' % [name, addressFlag, value, writePlaceholder]
      else
        null;

    local validateKafkaTopic(name, compartment, container) =
      local value = flagValue(container, topicFlag);
      if value == null then
        null
      else if compartment.kind == 'read' then
        if std.length(std.findSubstr(readPlaceholder, value)) > 0 then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but read compartment %d consumes a single topic (the topic must contain "-rc-%d", not the "%s" placeholder).' % [name, topicFlag, value, compartment.id, compartment.id, readPlaceholder]
        else if !containsIdToken(value, '-rc-%d' % compartment.id) then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but read compartment %d consumes a single topic (the topic must contain "-rc-%d").' % [name, topicFlag, value, compartment.id, compartment.id]
        else
          null
      else if std.length(std.findSubstr(readPlaceholder, value)) == 0 then
        if compartment.kind == 'write' then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but a write compartment produces to every read compartment\'s topic, so the topic must contain the "%s" placeholder.' % [name, topicFlag, value, readPlaceholder]
        else
          'The Deployment or StatefulSet "%s" sets "%s=%s", but a global deployment queries every read compartment\'s topic, so the topic must contain the "%s" placeholder.' % [name, topicFlag, value, readPlaceholder]
      else
        null;

    // The blocks-storage bucket is per read compartment. A read compartment must point at its own bucket
    // (name contains "-rc-<id>") or keep the "<read-compartment-id>" placeholder; any other deployment that
    // sets a blocks bucket must keep the placeholder, since one concrete bucket can't serve every compartment.
    local validateBlocksBucket(name, compartment, container) =
      local value = flagValue(container, blocksBucketFlag);
      if value == null then
        null
      else if compartment.kind != 'read' && !hasTarget(container, 'querier') && !hasTarget(container, 'ruler-querier') then
        null
      else if std.length(std.findSubstr(readPlaceholder, value)) > 0 then
        null
      else if compartment.kind == 'read' then
        if !containsIdToken(value, '-rc-%d' % compartment.id) then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but read compartment %d uses its own blocks-storage bucket (the bucket name must contain "-rc-%d" or the "%s" placeholder).' % [name, blocksBucketFlag, value, compartment.id, compartment.id, readPlaceholder]
        else
          null
      else
        'The Deployment or StatefulSet "%s" sets "%s=%s", but only a read compartment owns a dedicated blocks-storage bucket, so a bucket name here must contain the "%s" placeholder to address every read compartment.' % [name, blocksBucketFlag, value, readPlaceholder];

    local validateRulerRemoteEvaluation(name, compartment, container) =
      local value = flagValue(container, rulerQueryFrontendAddressFlag);
      if !hasTarget(container, 'ruler') then
        null
      else if value == null || value == '' then
        'The Deployment or StatefulSet "%s" runs "%s=%s", but compartments require a non-empty "%s=..." so the ruler evaluates queries through the ruler query-frontend.' % [name, targetFlag, flagValue(container, targetFlag), rulerQueryFrontendAddressFlag]
      else
        null;

    // A "<kind>-compartment-id" flag must appear only on a matching compartment, and its value must
    // equal the id encoded in the resource name.
    local validateCompartmentId(name, compartment, container, suffix, kind) =
      local found = flagBySuffix(container, suffix);
      if found == null then
        null
      else if compartment.kind != kind then
        'The Deployment or StatefulSet "%s" sets "%s=%s", but it does not belong to a %s compartment.' % [name, found.flag, found.value, kind]
      else if found.value != std.toString(compartment.id) then
        'The Deployment or StatefulSet "%s" sets "%s=%s", but it belongs to %s compartment %d (the id must match the compartment).' % [name, found.flag, found.value, compartment.kind, compartment.id]
      else
        null;

    local validateWriteCompartmentId(name, compartment, container) =
      validateCompartmentId(name, compartment, container, writeIdSuffix, 'write');

    local validateReadCompartmentId(name, compartment, container) =
      validateCompartmentId(name, compartment, container, readIdSuffix, 'read');

    local validateRootFlag(name, container, expected) =
      local value = flagValue(container, expected.flag);
      if value == null then
        'The Deployment or StatefulSet "%s" is missing "%s=%s" on the Mimir container with "%s=%s".' % [name, expected.flag, expected.value, targetFlag, flagValue(container, targetFlag)]
      else if value != expected.value then
        'The Deployment or StatefulSet "%s" sets "%s=%s", but compartments require "%s=%s" on every Mimir container.' % [name, expected.flag, value, expected.flag, expected.value]
      else
        null;

    local validateRootFlags(name, container) =
      std.foldl(
        function(firstError, expected) if firstError != null then firstError else validateRootFlag(name, container, expected),
        rootFlags,
        null
      );

    local isMimirContainer(container) =
      flagValue(container, targetFlag) != null;

    local validateContainer(name, compartment, container) =
      local compartmentError = std.foldl(
        function(firstError, validator) if firstError != null then firstError else validator(name, compartment, container),
        [validateKafkaAddress, validateKafkaTopic, validateBlocksBucket, validateRulerRemoteEvaluation, validateWriteCompartmentId, validateReadCompartmentId],
        null
      );
      if compartmentError != null then
        compartmentError
      else if requireCommonFlags && isMimirContainer(container) then
        validateRootFlags(name, container)
      else
        null;

    local validateResource(resource) =
      if resource == null then
        null
      else
        local name = resource.metadata.name;
        local compartment = extractCompartment(name);
        local containers = jsonpath.getJSONPath(resource, 'spec.template.spec.containers', []);
        local containerError = std.foldl(
          function(firstError, container) if firstError != null then firstError else validateContainer(name, compartment, container),
          containers,
          null
        );
        local mimirContainers = std.filter(isMimirContainer, containers);
        if containerError != null then
          containerError
        else if requireCommonFlags && std.length(containers) > 0 && std.length(mimirContainers) == 0 then
          'The Deployment or StatefulSet "%s" has containers but none of them look like a Mimir workload container with a "%s=..." CLI arg.' % [name, targetFlag]
        else
          null;

    local getResourceName(value) =
      if std.isObject(value) && std.objectHas(value, 'metadata') && std.objectHas(value.metadata, 'name') then value.metadata.name else null;

    local validateResourceSet(resourceOrSet) =
      if getResourceName(resourceOrSet) != null then
        validateResource(resourceOrSet)
      else if std.isObject(resourceOrSet) then
        std.foldl(
          function(firstError, key) if firstError != null then firstError else validateResource(resourceOrSet[key]),
          std.objectFields(resourceOrSet),
          null
        )
      else
        null;

    std.foldl(
      function(firstError, resourceName)
        if firstError != null then firstError
        else if !std.objectHas(root, resourceName) then 'validateMimirCompartmentsConfig expected root resource "%s", but it does not exist.' % resourceName
        else validateResourceSet(root[resourceName]),
      resourceNames,
      null
    ),
}
