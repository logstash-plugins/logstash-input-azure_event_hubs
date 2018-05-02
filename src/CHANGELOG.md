## 2016.05.03
* Fixed the jar dependency problem. Now installing the plugin by using the "plugin" or "logstash-plugin" (for newer versions of Logstash) should automatically download and jar files needed.
* Fixed the default value of *time_since_epoch_millis* to use UTC time to match the SelectorFilter.
* Made the plugin to respect Logstash shutdown signal.
* Updated the *logstash-core* runtime dependency requirement to '~> 2.0'.
* Updated the *logstash-devutils* development dependency requirement to '>= 0.0.16'