variable "credentials" {
  type = string
}
variable "project_id" {
  type = string
}

variable "instance_name" {
  type = string
}

variable "instance_type" {
  type = string
  default = "n1-standard-1"
}

variable "bucket_id" {
  type = string
}

variable "dataset_id" {
  type = string
}

