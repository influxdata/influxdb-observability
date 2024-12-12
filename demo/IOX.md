# Local Dev

To get this to work w/ a local copy of IOx:

Terminal 1:

```console
$ docker-compose up
```

Terminal 2:

```console
$ # go to iox checkout
$ cd my/iox/checkout

$ # clean state
$ rm -rf ~/.influxdb_iox

$ # start IOx
$ # need to bind to all interfaces for docker/podman
$ cargo run -- run all-in-one -vv --catalog-dsn=memory --router-http-bind=0.0.0.0:8080 --querier-grpc-bind=0.0.0.0:8082
```

Now visit the test app and generate some traces:

<http://localhost:8090/>

Then open the Jaeger UI:

<http://localhost:16686/>

**NOTE: It may take a few minutes for the Jaeger UI to show the traces. This is currently a caching issue in IOx.**
