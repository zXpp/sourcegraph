# Precise code intelligence converter

The docker image for this part of the application wraps a one or more converters [goreman](https://github.com/mattn/goreman) supervisor. By default, there is are two converter processes. The number of converters can be tuned with the environment variable `NUM_CONVERTERS`.

### Prometheus metrics

The precise-code-intel-converter exposes a metrics server (but nothing else interesting) on port 3188. It's possible to run multiple converters, but impossible for them all to serve metrics from the same port. Therefore, this container also includes a minimally-configured Prometheus process that will scrape metrics from all of the processes. It is suggested that you use [federation](https://prometheus.io/docs/prometheus/latest/federation/) to scrape all of the process metrics at once instead of scraping the individual ports directly. Doing so will ensure that scaling up or down the number of converters will not change the the required Prometheus configuration.
