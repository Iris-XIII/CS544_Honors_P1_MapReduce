# Docker Compose smoke test (single VM)

This is a **baseline deployment** of one Boss and four Workers, each limited to
one CPU. The Client CLI runs **on the VM host**, outside all containers. The
containers share files via a bind mount from `./shared` on the VM to `/shared`
inside each container. Speculative execution is not implemented by this setup.

## First run

From the repository root on the VM (Docker and Docker Compose plugin required):

```bash
mkdir -p shared/input shared/jobs shared/intermediate shared/output
cp examples/data/input.txt shared/input/input.txt
cp examples/wordcount.py shared/jobs/wordcount.py
# Use your VM account's numeric UID/GID, so container-written files are editable on the host.
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose config --quiet
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose up --build -d
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose ps
```

You should see a healthy Boss and four healthy Workers. If startup fails:

```bash
docker compose logs --tail=100 boss worker1 worker2 worker3 worker4
```

The Boss listens on the VM's loopback interface at `localhost:50050`. Workers
listen on port `50051` **inside the Compose network**, addressed by the Boss as
`worker1:50051`, ..., `worker4:50051`. Worker ports are not published to the host.

## Submit a real job from outside Docker

After merging the separate Client CLI branch, install the client dependencies in
an environment on the VM host (the Client CLI is **not** in the `main` snapshot
used to develop this Compose setup):

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python client.py \
  --boss localhost:50050 \
  --input /shared/input/input.txt \
  --output /shared/output/wordcount-1 \
  --job /shared/jobs/wordcount.py \
  --maps 4 --reduces 2
```

The **arguments** are the absolute paths *inside the containers*, even though
`client.py` itself runs on the VM host. The host-side files are
`./shared/input/input.txt` and `./shared/jobs/wordcount.py`. Do not pass the
host's `~/.../shared` paths to the Boss.

When the client reports `COMPLETED`, inspect the results on the VM host:

```bash
cat shared/output/wordcount-1/part-*.jsonl
```

The example should count: `hello=2`, `world=3`, `mapreduce=1`,
`distributed=1`, and `computing=1` (ordering across reducer files may vary).
Use a fresh output directory such as `wordcount-2` for each new job; the Boss
will not overwrite a nonempty output directory.

## Stop and clean up

```bash
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose down
```

`docker compose down` does **not** delete the bind-mounted `./shared` directory.
Remove old output/intermediate data yourself only after confirming it is no
longer needed. The Boss stores job metadata only in memory, so restarting it
loses existing job IDs/statuses.

## Constraints

- This configuration runs all five containers on **one VM** as required. It is
  not a multi-host distributed deployment.
- One CPU limit applies to **each of the four Worker containers** (`cpus: 1.0`).
  The Boss is not given that limit.
- Health checks test that the server TCP ports are accepting connections; they
  do not replace an end-to-end gRPC test.
- The image uses Python 3.12 and pinned gRPC/Protobuf dependencies.
- Worker-side failures, task cancellation, and speculative execution remain
  outside the scope of this deployment PR.
