output "bucket_name" {
  description = "Name of the private S3 lakehouse bucket."
  value       = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  description = "ARN of the private S3 lakehouse bucket."
  value       = aws_s3_bucket.lakehouse.arn
}

output "layer_uris" {
  description = "Canonical S3 prefixes for pipeline data and operational metadata."
  value = {
    audit    = "s3://${aws_s3_bucket.lakehouse.id}/audit/"
    bronze   = "s3://${aws_s3_bucket.lakehouse.id}/bronze/"
    gold     = "s3://${aws_s3_bucket.lakehouse.id}/gold/"
    rejected = "s3://${aws_s3_bucket.lakehouse.id}/rejected/"
    silver   = "s3://${aws_s3_bucket.lakehouse.id}/silver/"
  }
}

output "runtime_iam_policy_json" {
  description = "Least-privilege policy to attach to the Databricks or pipeline runtime identity."
  value       = data.aws_iam_policy_document.lakehouse_access.json
}
