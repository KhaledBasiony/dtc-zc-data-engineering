variable "db_version" {
  description = "The Postgresql version used in my project"
  default     = "17"
}

variable "db_port" {
  type        = number
  description = "The port on the host machine that binds to the database"
  default     = 5450
}

variable "db_conn" {
  type = object({
    username = string
    password = string
    db_name  = string
  })
  description = "The information describing a database connection"
  default = {
    username = "postgres"
    password = "postgres"
    db_name  = "tf_db"
  }
}

variable "admin_port" {
  type        = number
  description = "The port on the host machine that binds to the admin interface"
  default     = 8098
}

variable "admin_login" {
  type = object({
    email    = string
    password = string
  })
  description = "The information for default admin ui login"
  default = {
    email    = "pgadmin@pgadmin.com"
    password = "pgadmin"
  }
}
