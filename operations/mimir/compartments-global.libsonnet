{
  // The base config sets the -ingest-storage.kafka.* settings on every Mimir component (through
  // $._config.commonConfig), even on components that don't use them. Keep that consistency under
  // compartments: these global components carry the compartments flags and the per-compartment Kafka
  // topic, parameterised with the '<read-compartment-id>' placeholder.
  local compartmentArgs = if !$._config.compartments_enabled then {} else $.mimirCompartmentsCommonArgs,

  alertmanager_args+:: compartmentArgs,
  query_scheduler_args+:: compartmentArgs,
  ruler_query_scheduler_args+:: compartmentArgs,
  overrides_exporter_args+:: compartmentArgs,

  // Config validation.
  local globalCompartmentConfigError = if !$._config.compartments_enabled then null else $.validateMimirCompartmentsConfig([
    'alertmanager_statefulset',
    'query_scheduler_deployment',
    'query_scheduler_zone_a_deployment',
    'query_scheduler_zone_b_deployment',
    'query_scheduler_zone_c_deployment',
    'ruler_query_scheduler_deployment',
    'ruler_query_scheduler_zone_a_deployment',
    'ruler_query_scheduler_zone_b_deployment',
    'ruler_query_scheduler_zone_c_deployment',
    'overrides_exporter_deployment',
  ], requireCommonFlags=true),
  assert globalCompartmentConfigError == null : globalCompartmentConfigError,
}
