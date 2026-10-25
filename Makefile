# floe: one-command local platform on kind.
CLUSTER ?= floe
NS      ?= floe
IMAGE   ?= floe
TAG     ?= dev
KEDA_VERSION ?= 2.21.0
HELM_ARGS ?=
PY ?= python3

.PHONY: up down cluster image load keda deploy status e2e bench grafana api test lint helm-check

up: cluster image load keda deploy status ## create cluster, build + load image, install KEDA and floe

cluster:
	@kind get clusters 2>/dev/null | grep -qx $(CLUSTER) || kind create cluster --config deploy/kind/cluster.yaml --name $(CLUSTER) --wait 120s

image:
	docker build -f docker/Dockerfile -t $(IMAGE):$(TAG) .

load:
	kind load docker-image $(IMAGE):$(TAG) --name $(CLUSTER)

keda:
	helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
	helm repo update kedacore >/dev/null
	helm upgrade --install keda kedacore/keda --version $(KEDA_VERSION) -n keda --create-namespace \
	  -f deploy/keda-values.yaml --wait --timeout 5m

deploy:
	helm upgrade --install floe deploy/helm/floe -n $(NS) --create-namespace \
	  --set image.repository=$(IMAGE) --set image.tag=$(TAG) $(HELM_ARGS) --wait --timeout 10m

status:
	kubectl -n $(NS) get pods,svc,scaledobject,cronjob

e2e: ## push events through the deployed stack and assert they land exactly once with quality checks passing
	$(PY) tests/e2e/run_e2e.py --namespace $(NS) --image $(IMAGE):$(TAG)

bench: ## throughput, latency, autoscaling reaction and pod-kill recovery
	$(PY) bench/run_bench.py --namespace $(NS) --image $(IMAGE):$(TAG)

grafana:
	kubectl -n $(NS) port-forward svc/floe-grafana 3000:3000

api:
	kubectl -n $(NS) port-forward svc/floe-api 8000:8000

test:
	pytest -q

lint:
	ruff check src tests bench deploy

helm-check:
	helm lint deploy/helm/floe
	helm template floe deploy/helm/floe -n $(NS) | kubeconform -strict -summary -ignore-missing-schemas

down:
	kind delete cluster --name $(CLUSTER)
