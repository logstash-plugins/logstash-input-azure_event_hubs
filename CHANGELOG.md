## 1.4.7
  - [DOCS] Clarify examples for single and multiple event hubs [#90](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/90)

## 1.4.6
  - [DOCS] Add outbound port requirements for Event Hub [#88](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/88)

## 1.4.5
 - Upgrade multiple dependencies such as `gson`, `log4j2`, `jackson` to make the plugin stable [#83](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/83)

## 1.4.4
 - Fix: Replace use of block with lambda to fix wrong number of arguments error on jruby-9.3.4.0 [#75](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/75)

## 1.4.3
 - Build: make log4j-api a provided dependency [#73](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/73)

## 1.4.2
 - Update log4j dependencies to 2.17.0

## 1.4.1
 - Update log4j dependencies [#71](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/71)
 - Fixed Gradle's script to use Gradle 7 [#69](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/69)

## 1.4.0
 - Updated the minor version of Azure SDK and other dependencies to ensure users of this plugin get upstream fixes and improvements [#67](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/67)

## 1.3.0
 - Add EventHub `user properties` in `@metadata` object [#66](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/66)

## 1.2.3
 - Fixed missing configuration of `prefetch_count` and `receive_timeout` [#61](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/61)

## 1.2.2
 - Refactor: scope and review global java_imports [#57](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/57)

## 1.2.1
 - [DOC] Changed documentation to update the default number of threads [#55](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/55)

## 1.2.0
 - Changed the default number of threads from `4` to `16` to match the default number from the Azure-Sdk EventProcessorHost [#54](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/54)

## 1.1.4
 - Fixed missing configuration of the `max_batch_size`setting [#52](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/52)

## 1.1.3
 - [DOC] Added clarification for threads parameter [#50](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/50)

## 1.1.2
 - Added workaround to fix errors when using this plugin with Java 11[#38](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/38)

## 1.1.1
 - Updated Azure event hub library dependencies[#36](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/36)

## 1.1.0
 - Updated Azure event hub library dependencies[#27](https://github.com/logstash-plugins/logstash-input-azure_event_hubs/pull/27)

## 1.0.4
 - Added guidelines for setting number of threads

## 1.0.3
 - Fixed doc issues
   - Changed `event-hub-connections` to `event_hub_connections`
   - Added emphasis for singular vs. plural for event_hub_connections

## 1.0.2
 - Fixed minor doc issues
 - Changed doc to hardcode `Logstash` rather than using an attribute.

## 1.0.1
 - Fixed logging for exception handler

## 1.0.0
 - Initial release of `logstash-input-azure_event_hubs` supersedes `logstash-input-azureeventhub`
 - Re-implementation that uses Event Processor Host API and new configuration options.
