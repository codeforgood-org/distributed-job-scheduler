module github.com/codeforgood-org/distributed-job-scheduler

go 1.21

require (
	github.com/dgraph-io/badger/v3 v3.2103.5
	github.com/gin-gonic/gin v1.9.1
	github.com/google/uuid v1.5.0
	github.com/hashicorp/raft v1.6.0
	github.com/hashicorp/raft-boltdb/v2 v2.3.0
	github.com/prometheus/client_golang v1.17.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.8.4
	go.uber.org/zap v1.26.0
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.31.0
	gopkg.in/yaml.v3 v3.0.1
)
