terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "3.6.2"
    }
  }
}

provider "docker" {

}

resource "docker_image" "db_img" {
  name = "postgres:${var.db_version}-alpine"
}

resource "docker_image" "admin_img" {
  name = "dpage/pgadmin4:latest"
}

resource "docker_container" "db" {
  image = docker_image.db_img.name
  name  = "tf-db"
  env = [
    "POSTGRES_USER=${var.db_conn.username}",
    "POSTGRES_PASSWORD=${var.db_conn.password}",
    "POSTGRES_DB=${var.db_conn.db_name}",
  ]
  ports {
    internal = 5432
    external = var.db_port
  }
}

resource "docker_container" "admin" {
  image = docker_image.admin_img.name
  name  = "tf-admin-ui"
  env = [
    "PGADMIN_DEFAULT_EMAIL=${var.admin_login.email}",
    "PGADMIN_DEFAULT_PASSWORD=${var.admin_login.password}"
  ]
  ports {
    internal = 80
    external = var.admin_port
  }
}
