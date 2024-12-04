FROM docker.elastic.co/logstash/logstash:8.16.0

COPY . .
ADD ./gems /etc/logstash/gems/

USER root
ENV JAVA_HOME=/usr/share/logstash/jdk
RUN /usr/share/logstash/bin/ruby -S gem uninstall bundler
RUN /usr/share/logstash/bin/ruby -S gem install bundler -v 2.5.23

RUN bin/logstash-plugin install --preserve --local --no-verify /etc/logstash/gems/logstash-input-azure_event_hubs_uipath-1.5.0.gem