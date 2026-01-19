# Solutions and Notes

## Docker & SQL

> ## Question 1. Understanding Docker images
>
> Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.
>
> What's the version of `pip` in the image?
>
> - 25.3
> - 24.3.1
> - 24.2.1
> - 23.3.1

### Solution 1.

How I solved this is by running

```bash
docker run --rm -it python:3.13.11-slim bash -c 'pip --version'
```

or by invoking the [script](py-docker-launcher.sh) using my shell then adding the command to execute inside the container, example:

```bash
zsh py-docker-launcher.sh bash -c 'pip --version'
```

So the answer I got is

    25.3

> ## Question 2. Understanding Docker networking and docker-compose
> 
> Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?
> 
> ```yaml
> services:
>   db:
>     container_name: postgres
>     image: postgres:17-alpine
>     environment:
>       POSTGRES_USER: 'postgres'
>       POSTGRES_PASSWORD: 'postgres'
>       POSTGRES_DB: 'ny_taxi'
>     ports:
>       - '5433:5432'
>     volumes:
>       - vol-pgdata:/var/lib/postgresql/data
> 
>   pgadmin:
>     container_name: pgadmin
>     image: dpage/pgadmin4:latest
>     environment:
>       PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
>       PGADMIN_DEFAULT_PASSWORD: "pgadmin"
>     ports:
>       - "8080:80"
>     volumes:
>       - vol-pgadmin_data:/var/lib/pgadmin
> 
> volumes:
>   vol-pgdata:
>     name: vol-pgdata
>   vol-pgadmin_data:
>     name: vol-pgadmin_data
> ```
> 
> - postgres:5433
> - localhost:5432
> - db:5433
> - postgres:5432
> - db:5432
> 
> If multiple answers are correct, select any 

### Solution 2.

to solve this I copied the content to a file named `docker-compose.yml` (.yaml also works) then ran the following command:
```bash
docker compose up -d
```
Note: -d is for detached mode, so I could get back the terminal back.

then by trial and error, both of those seem to work:

    - db:5432
    - postgres5432

Notes: 
- This is using the **internal** port of the container not the **published** port, since they are running on the same network
- You can use the service name or the container name (or other things as well) but keep in mind that there differences when scaling with multiple containers.

> ## Question 3. Counting short trips
> 
> For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?
> 
> - 7,853
> - 8,007
> - 8,254
> - 8,421

### Solution 3.

    8,007

details in the [notebook](notebooks/hw_mod_1.ipynb)

> ## Question 4. Longest trip for each day
> 
> Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).
> 
> Use the pick up time for your calculations.
> 
> - 2025-11-14
> - 2025-11-20
> - 2025-11-23
> - 2025-11-25

### Solution 4.

    2025-11-14

details in the [notebook](notebooks/hw_mod_1.ipynb)

> ## Question 5. Biggest pickup zone
> 
> Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?
> 
> - East Harlem North
> - East Harlem South
> - Morningside Heights
> - Forest Hills

### Solution 5.

    East Harlem North

details in the [notebook](notebooks/hw_mod_1.ipynb)

> ## Question 6. Largest tip
> 
> For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
> 
> Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.
> 
> - JFK Airport
> - Yorkville West
> - East Harlem North
> - LaGuardia Airport

### Solution 6.

    Yorkville West

details in the [notebook](notebooks/hw_mod_1.ipynb)

## Terraform

Too bad I couldn't get a trial account, I will try to work with terraform locally instead.

> ## Question 7. Terraform Workflow
> 
> Which of the following sequences, respectively, describes the workflow for:
> 1. Downloading the provider plugins and setting up backend,
> 2. Generating proposed changes and auto-executing the plan
> 3. Remove all resources managed by terraform`
> 
> Answers:
> - terraform import, terraform apply -y, terraform destroy
> - teraform init, terraform plan -auto-apply, terraform rm
> - terraform init, terraform run -auto-approve, terraform destroy
> - terraform init, terraform apply -auto-approve, terraform destroy
> - terraform import, terraform apply -y, terraform rm

## Solution 7.

    terraform init, terraform apply -auto-approve, terraform destroy

Since I don't have a cloud account to test on, I tried to replicate the postgres/pgadmin setup using terraform, but it behaves differently than the docker compose way, they are NOT on the same network.

so the database container must publish its port to the host `5450:5432` for example, and the pgadmin must connect to the host on the said port `5450`

Note:
- from the pgadmin, the host would be `172.17.0.1` which the default ip address docker assigns to the host machine. if you try connecting to a database on localhost it will not find it, because in the pgadmin's container _*THERE IS NO POSTGRES*_ (I always tend forget that detail)
- other network configurations is definitely possible, this is just the default behaviour for isolated containers using the default `bridged` network. if you wish you can search more on [docker networks](https://docs.docker.com/engine/network/) or [tf docker_container network](https://registry.terraform.io/providers/kreuzwerker/docker/latest/docs/resources/container#nestedblock--networks_advanced)