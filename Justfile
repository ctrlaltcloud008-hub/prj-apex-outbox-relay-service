build:
  docker buildx build --no-cache -t prj-apex-outbox-poller-service .

network:
  docker network create spanner-net

run:
  docker run -d -v $HOME/.config/gcloud:/tmp/gcloud:ro -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcloud/application_default_credentials.json -e PUBSUB_EMULATOR_HOST=host.docker.internal:8085  --name prj-apex-outbox-poller-service --network spanner-net -p 8000:8000 -e SPANNER_EMULATOR_HOST=spanner-emulator:9010 prj-apex-outbox-poller-service


spanner-up:
  docker run --name spanner-emulator --network spanner-net -p 9010:9010 -p 9020:9020 -d gcr.io/cloud-spanner-emulator/emulator

migrate-up:
  export SPANNER_EMULATOR_HOST=localhost:9010 && export SPANNER_PROJECT_ID=test-project && export SPANNER_INSTANCE_ID=test-instance && export SPANNER_DATABASE_ID=test-database  && wrench migrate up --directory schema

emulator-config:
  gcloud config configurations create emulator 2>/dev/null || true
  gcloud config configurations activate emulator
  gcloud config set auth/disable_credentials true
  gcloud config set project test-project
  gcloud config set api_endpoint_overrides/spanner http://localhost:9020/

spanner-create:
  gcloud spanner instances create test-instance --config=regional-us-central1 --description="Local Instance" --nodes=1
  gcloud spanner databases create test-database --instance test-instance

spanner-cli:
  SPANNER_EMULATOR_HOST=localhost:9010 spanner-cli sql --project test-project --instance test-instance --database test-database


pubsub-up:
	gcloud beta emulators pubsub start --project=test-project --host-port=0.0.0.0:8085 &
	sleep 2
	echo "Pub/Sub emulator ready on 0.0.0.0:8085"

pubsub-init:
	curl -X PUT http://localhost:8085/v1/projects/test-project/topics/video.validated
	curl -X PUT http://localhost:8085/v1/projects/test-project/subscriptions/test-subscription \
	  -H "Content-Type: application/json" \
	  -d '{"topic": "projects/test-project/topics/video.validated"}'


fmt:
  go fmt ./...

vet:
  go vet ./...

lint:
  golangci-lint run

test:
  go test ./... -v

