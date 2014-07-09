## Opentsdb - Riemann Publisher

RTPublisher Plugin for Opentsdb 2.0 to send metrics to Riemann

### Usage
Compile it with 'mvn compile && mvn assembly:single'

Add these settings to opentsdb.conf  
support multi riemann hosts,  now use hash(service) to achive load balance.

    tsd.core.plugin_path = $plugin_dir
    tsd.rtpublisher.enable = true
    tsd.rtpublisher.plugin = at.ac.uniak.tsdb.RiemannPublisher
    tsd.plugin.riemann.port = $riemann_tcp_port
    tsd.plugin.riemann.host = $riemann_ip1,$riemann_ip2,...
