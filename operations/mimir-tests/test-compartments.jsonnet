// Renders Mimir with the experimental compartments architecture enabled, deployed multi-AZ across all
// components (write and read path).
local env = (import 'test-ingest-storage-autoscaling-one-trigger.jsonnet') {
  _config+:: {
    multi_zone_availability_zones: ['us-east-2a', 'us-east-2b'],
    ingest_storage_ingester_zones: 2,

    // Multi-AZ write and read path.
    multi_zone_write_path_enabled: true,
    multi_zone_read_path_enabled: true,
    multi_zone_read_path_multi_az_enabled: true,
    multi_zone_memberlist_bridge_enabled: true,
    memberlist_zone_aware_routing_enabled: true,
    query_scheduler_service_discovery_mode: 'ring',

    // Exercise the per-compartment distributor scaled objects.
    autoscaling_distributor_enabled: true,
    autoscaling_distributor_min_replicas_per_zone: 2,
    autoscaling_distributor_max_replicas_per_zone: 10,

    // Compartments.
    compartments_enabled: true,
    compartments_read_count: 2,
    compartments_write_count: 2,

    compactor_scheduler_enabled: true,
    autoscaling_compactor_enabled: true,
    autoscaling_compactor_min_replicas: 2,
    autoscaling_compactor_max_replicas: 30,
    cortex_compactor_concurrent_rollout_enabled: true,
    enable_pvc_auto_deletion_for_compactors: true,
    enable_pvc_auto_deletion_for_ingesters: true,

    // Exercise the per-compartment store-gateways (zones a/b/c, no backup zones).
    multi_zone_store_gateway_enabled: true,
    multi_zone_store_gateway_replicas: 3,
    autoscaling_store_gateway_enabled: true,
    autoscaling_store_gateway_min_replicas_per_zone: 1,
    autoscaling_store_gateway_max_replicas_per_zone: 6,
    autoscaling_store_gateway_min_replicas_per_compartment_zone: 1,
    autoscaling_store_gateway_max_replicas_per_compartment_zone: 3,
    enable_pvc_auto_deletion_for_store_gateways: true,

    // Exercise global Mimir workload compartments flags.
    overrides_exporter_enabled: true,
  },
};

local all(values) = std.foldl(function(acc, value) acc && value, values, true);
local any(values) = std.foldl(function(acc, value) acc || value, values, false);
local concat(arrays) = std.foldl(function(acc, value) acc + value, arrays, []);
local isEmptyObject(value) = std.isObject(value) && std.length(std.objectFields(value)) == 0;
local getField(name) = if std.objectHas(env, name) then env[name] else null;
local resourceName(resource) =
  if std.isObject(resource) && std.objectHas(resource, 'metadata') && std.objectHas(resource.metadata, 'name')
  then resource.metadata.name
  else '<unnamed>';
local resourceContainers(resource) =
  if resource == null || isEmptyObject(resource) then []
  else if std.objectHas(resource, 'spec') &&
          std.objectHas(resource.spec, 'template') &&
          std.objectHas(resource.spec.template, 'spec') &&
          std.objectHas(resource.spec.template.spec, 'containers') then
    resource.spec.template.spec.containers
  else [];
local resourceList(resourceOrSet) =
  if resourceOrSet == null || isEmptyObject(resourceOrSet) then []
  else if std.isObject(resourceOrSet) &&
          std.objectHas(resourceOrSet, 'metadata') &&
          std.objectHas(resourceOrSet.metadata, 'name') then
    [resourceOrSet]
  else if std.isObject(resourceOrSet) then
    concat([resourceList(resourceOrSet[key]) for key in std.objectFields(resourceOrSet)])
  else
    [];
local flagValue(container, flag) =
  if !std.objectHas(container, 'args') then
    null
  else
    local prefix = flag + '=';
    local matching = std.filter(function(arg) std.isString(arg) && std.startsWith(arg, prefix), container.args);
    if std.length(matching) == 0 then null else std.substr(matching[0], std.length(prefix), std.length(matching[0]));
local rootFlags = [
  '-compartments.enabled=true',
  '-compartments.read.num-compartments=2',
  '-compartments.write.num-compartments=2',
];
local rootFlagNames = [
  '-compartments.enabled',
  '-compartments.read.num-compartments',
  '-compartments.write.num-compartments',
];
local mimirContainers(resource) =
  [container for container in resourceContainers(resource) if flagValue(container, '-target') != null];
local hasRootFlags(resource) =
  local containers = mimirContainers(resource);
  std.length(containers) > 0 &&
  all([std.member(container.args, flag) for container in containers for flag in rootFlags]);
local hasAnyRootFlag(resource) =
  any([
    std.isString(arg) && any([std.startsWith(arg, flag + '=') for flag in rootFlagNames])
    for container in resourceContainers(resource)
    if std.objectHas(container, 'args')
    for arg in container.args
  ]);
// Every Mimir workload inherits the -ingest-storage.kafka.* settings from the common config, so any
// topic it carries must be the per-read-compartment form: a concrete "ingest-rc-<n>" on per-compartment
// workloads, or the "<read-compartment-id>" placeholder on global ones.
local hasCompartmentKafkaTopics(resource) =
  all([
    std.startsWith(flagValue(container, '-ingest-storage.kafka.topic'), 'ingest-rc-')
    for container in mimirContainers(resource)
    if flagValue(container, '-ingest-storage.kafka.topic') != null
  ]);
local rulerDistributorAddress(zone) =
  'dns:///distributor-zone-%s-compartments.%s.svc.%s:9095' % [zone, env._config.namespace, env._config.cluster_domain];

local mimirWorkloadResources = concat([
  resourceList(resource)
  for resource in [
    env.alertmanager_statefulset,
    env.distributor_zone_a_deployments,
    env.distributor_zone_b_deployments,
    env.distributor_zone_c_deployments,
    env.ingester_zone_a_statefulsets,
    env.ingester_zone_b_statefulsets,
    env.ingester_zone_c_statefulsets,
    env.querier_deployment,
    env.querier_zone_a_deployment,
    env.querier_zone_b_deployment,
    env.querier_zone_c_deployment,
    env.query_frontend_deployment,
    env.query_frontend_zone_a_deployment,
    env.query_frontend_zone_b_deployment,
    env.query_frontend_zone_c_deployment,
    env.query_scheduler_deployment,
    env.query_scheduler_zone_a_deployment,
    env.query_scheduler_zone_b_deployment,
    env.query_scheduler_zone_c_deployment,
    env.ruler_deployment,
    env.ruler_zone_a_deployment,
    env.ruler_zone_b_deployment,
    env.ruler_zone_c_deployment,
    env.ruler_querier_deployment,
    env.ruler_querier_zone_a_deployment,
    env.ruler_querier_zone_b_deployment,
    env.ruler_querier_zone_c_deployment,
    env.ruler_query_frontend_deployment,
    env.ruler_query_frontend_zone_a_deployment,
    env.ruler_query_frontend_zone_b_deployment,
    env.ruler_query_frontend_zone_c_deployment,
    env.ruler_query_scheduler_deployment,
    env.ruler_query_scheduler_zone_a_deployment,
    env.ruler_query_scheduler_zone_b_deployment,
    env.ruler_query_scheduler_zone_c_deployment,
    env.compactor_statefulsets,
    env.compactor_scheduler_statefulsets,
    env.store_gateway_zone_a_statefulsets,
    env.store_gateway_zone_b_statefulsets,
    env.store_gateway_zone_c_statefulsets,
    env.store_gateway_zone_a_backup_statefulsets,
    env.store_gateway_zone_b_backup_statefulsets,
    env.memberlist_bridge_zone_a_deployment,
    env.memberlist_bridge_zone_b_deployment,
    env.memberlist_bridge_zone_c_deployment,
    env.overrides_exporter_deployment,
  ]
]);

local helperResources = concat([
  resourceList(resource)
  for resource in [
    getField('rollout_operator_deployment'),
    env.memcached_frontend,
    env.memcached_frontend_zone_a,
    env.memcached_frontend_zone_b,
    env.memcached_frontend_zone_c,
    env.memcached_index_queries,
    env.memcached_index_queries_zone_a,
    env.memcached_index_queries_zone_b,
    env.memcached_index_queries_zone_c,
    env.memcached_index_queries_zone_a_compartments,
    env.memcached_index_queries_zone_b_compartments,
    env.memcached_index_queries_zone_c_compartments,
    env.memcached_chunks,
    env.memcached_chunks_zone_a,
    env.memcached_chunks_zone_b,
    env.memcached_chunks_zone_c,
    env.memcached_chunks_zone_a_compartments,
    env.memcached_chunks_zone_b_compartments,
    env.memcached_chunks_zone_c_compartments,
    env.memcached_metadata,
    env.memcached_metadata_zone_a,
    env.memcached_metadata_zone_b,
    env.memcached_metadata_zone_c,
    env.memcached_range_vector_splitting,
    env.memcached_range_vector_splitting_zone_a,
    env.memcached_range_vector_splitting_zone_b,
    env.memcached_range_vector_splitting_zone_c,
    env.newMimirtoolListBlocksJob('compartments-mimirtool-no-root-flags'),
    (env {
       _config+:: {
         continuous_test_enabled: true,
         continuous_test_tenant_id: 'test',
         continuous_test_write_endpoint: 'http://distributor.default.svc.cluster.local',
         continuous_test_read_endpoint: 'http://query-frontend.default.svc.cluster.local',
       },
     }).continuous_test_deployment,
  ]
]);

assert std.length(mimirWorkloadResources) > 0 : 'expected active Mimir workload resources in compartments test';
assert all([hasRootFlags(resource) for resource in mimirWorkloadResources]) :
       'expected all active Mimir workloads to have compartments root flags; missing on: %s' % std.join(', ', [resourceName(resource) for resource in mimirWorkloadResources if !hasRootFlags(resource)]);
assert all([hasCompartmentKafkaTopics(resource) for resource in mimirWorkloadResources]) :
       'expected all active Mimir workloads to use per-read-compartment Kafka topics; plain topic on: %s' % std.join(', ', [resourceName(resource) for resource in mimirWorkloadResources if !hasCompartmentKafkaTopics(resource)]);
assert all([!hasAnyRootFlag(resource) for resource in helperResources]) :
       'expected helper/non-Mimir resources to avoid compartments root flags; found on: %s' % std.join(', ', [resourceName(resource) for resource in helperResources if hasAnyRootFlag(resource)]);

assert env.distributor_zone_a_compartments_service.metadata.name == 'distributor-zone-a-compartments' :
       'expected zone-a compartments distributor service name';
assert env.distributor_zone_a_compartments_service.spec.selector['mimir-service'] == 'distributor-zone-a' :
       'expected zone-a compartments distributor service to select compartment distributor pods';
assert env.distributor_zone_a_compartments_service.spec.clusterIP == 'None' :
       'expected zone-a compartments distributor service to be headless';
assert env.ruler_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected single-zone ruler to write to the zone-a compartments distributor service';
assert env.ruler_zone_a_args['ruler.distributor.address'] == rulerDistributorAddress('a') :
       'expected zone-a ruler to write to the zone-a compartments distributor service';
assert env.ruler_zone_b_args['ruler.distributor.address'] == rulerDistributorAddress('b') :
       'expected zone-b ruler to write to the zone-b compartments distributor service';
assert !std.objectHas(env.ruler_args, 'distributor.write-compartment-id') :
       'rulers must stay global and must not set distributor.write-compartment-id';
assert std.length(std.findSubstr('<read-compartment-id>', env.ruler_args[env.mimirBlocksStorageBucketNameFlag])) == 0 :
       'rulers must keep the normal ruler blocks bucket instead of a per-read-compartment bucket';

env
