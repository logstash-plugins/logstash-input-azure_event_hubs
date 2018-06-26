require 'logstash/devutils/rake'

task :default do
  system("rake -T")
end

task :install_jars do
  exit(1) unless system('./gradlew vendor')
end

task :vendor => :install_jars

