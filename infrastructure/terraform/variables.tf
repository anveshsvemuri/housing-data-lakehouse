variable "aws_region" {
  description = "AWS region for the lakehouse storage bucket."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Lowercase project identifier used in resource names and tags."
  type        = string
  default     = "housing-data-lakehouse"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "project_name may contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Deployment environment represented by these resources."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "test", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, staging, prod."
  }
}

variable "force_destroy" {
  description = "Allow Terraform to delete a non-empty bucket. Keep false outside disposable demos."
  type        = bool
  default     = false
}

variable "additional_tags" {
  description = "Additional tags merged onto every supported AWS resource."
  type        = map(string)
  default     = {}
}
