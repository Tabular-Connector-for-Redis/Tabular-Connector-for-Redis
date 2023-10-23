# Getting Started:

## Configuring for your Redis instance

The application's configuration is stored in the `conf` folder. To point the application to your Redis instance, change the `host` and `port` in `conf/env.yaml`.

The `prefix` in that file can be whatever you wish, and serves only to disambiguate the keys created by this application from other keys.

## Running the application

To run API 

```console
go run . 
```

To run tests, run tests in two different phases to support normal tests and redis search tests

Phase One : Run all tests that is using mini Redis (non redis search tests)
```console
make test
```

Phase Two : Run all tests that is using docker container (redis search tests)
```console
make test-search
```
# Routes

## Schemas

<!-- https://gist.github.com/azagniotov/a4b16faf0febd12efbc6c3d7370383a6 -->
<details>
 <summary><code>POST</code> <code><b>/api/v1/schema</b></code> <code>(adds a new schema to redis database)</code></summary>

##### Parameters

> | name      |  type     | data type               | description                                                           |
> |-----------|-----------|-------------------------|-----------------------------------------------------------------------|
> | None      |  required | JSON   | Schema data in JSON  |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `text/plain;charset=UTF-8`        | `Configuration created successfully`                                |
> | `400`         | `application/json`                | `{"code":"400","message":"error"`                            |

</details>

<details>
 <summary><code>GET</code> <code><b>/api/v1/schema/<b>{table}</b></code> <code>(returns schema for table)</code></summary>

##### Parameters

> None


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json;charset=UTF-8`        | JSON                               |
> | `400`         | `application/json`                | `{"code":"400","message":"error"`                       |

</details>

<details>
 <summary><code>GET</code> <code><b>/api/v1/schema</code> <code>(returns all schemas)</code></summary>

##### Parameters

> None


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json;charset=UTF-8`        | JSON                               |
> | `400`         | `application/json`                | `{"code":"400","message":"error"`                       |

</details>

## Data
<details>
 <summary><code>POST</code> <code><b>/api/v1/schema/<b>{table}</b>/load</code> <code>(new bulk load for table)</code></summary>

##### Parameters

> | name      |  type     | data type               | description                                                           |
> |-----------|-----------|-------------------------|-----------------------------------------------------------------------|
> | None      |  required | CSV   | Data in csv  |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json;charset=UTF-8`        | JSON                               |
> | `400`         | `application/json`                | `{"code":"400","message":"error"`                       |

</details>

<details>
 <summary><code>GET</code> <code><b>/api/v1/schema/<b>{table}</b>/data</code> <code>(return data for table)</code></summary>

##### Parameters

> | name      |  type     | data type               | description                                                           |
> |-----------|-----------|-------------------------|-----------------------------------------------------------------------|
> | None      |  optional | JSON   | Filters in JSON  |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json;charset=UTF-8`        | JSON                               |
> | `400`         | `application/json`                | `{"code":"400","message":"error"`                       |

</details>

<details>
 <summary><code>PATCH</code> <code><b>/api/v1/schema/<b>{table}</b>/update</code> <code>(update data for table)</code></summary>

##### Parameters

> | name      |  type     | data type               | description                                                           |
> |-----------|-----------|-------------------------|-----------------------------------------------------------------------|
> | None      |  required | JSON   | Filters and Updated values in JSON  |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json;charset=UTF-8`        | JSON                               |
> | `400`         | `application/json`                | `{"code":"400","message":"error"`                       |

</details># Tabular-Connector-for-Redis
