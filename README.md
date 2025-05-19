# prometheus-labels-db

`prometheus-labels-db` is a tool for efficiently retrieving label (dimension) information from AWS CloudWatch metrics and managing or utilizing them in Prometheus format.

It supports the Prometheus `/api/v1/series` API, enabling flexible search and use of CloudWatch metric information.

## Build

### Local build

```sh
go build -o recorder ./cmd/recorder/main.go ./cmd/recorder/recorder.go
go build -o query ./cmd/query/main.go
```

### Package build

```sh
# For release
docker build --output=dist --target=final .

# For snapshot
docker build --output=dist --target=final --build-arg SNAPSHOT=true .
```

## Usage

First, run the recorder to collect CloudWatch metrics information:

```sh
./recorder --config.file ./examples/config.yaml --db.dir="./data/" --oneshot
```

Next, start the query service to provide the API endpoint:

```sh
./query --db.dir="./data/"
```

To retrieve metric label information, send a request to the API as follows:

```sh
curl -sG "http://localhost:8080/api/v1/series" \
  --data-urlencode 'match[]=CPUUtilization{Namespace="AWS/EC2",Region="ap-northeast-1",InstanceId!=""}' \
  --data-urlencode "start=$(date +"%Y-%m-%dT%H:%M:%SZ" --date="1 day ago")" \
  --data-urlencode "end=$(date +"%Y-%m-%dT%H:%M:%SZ")"
```

## Testing

To run unit tests:

```sh
go test ./...
```

To inspect the actual data:

```sh
sqlite3 data/labels_20250428_20250720.db 'select * from metrics_20250428_20250720 limit 1'
sqlite3 data/labels_20250428_20250720.db 'select * from metrics_lifetime_20250428_20250720_AWS_EC2 limit 1'
```

## Limitations

- This tool uses PCRE (Perl Compatible Regular Expressions) for internal regular expression matching. As a result, some regular expression patterns that are valid in Go's RE2 engine may not be compatible or may behave differently in this tool. Please be aware of this difference when writing queries that include regular expressions.
