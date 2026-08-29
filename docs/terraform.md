# Terraform infrastructure

The Terraform module in `infrastructure/terraform` provisions one private S3 bucket as the storage
foundation for the housing lakehouse. It enables versioning, default server-side encryption,
bucket-owner-enforced ownership, full public-access blocking, and cleanup of incomplete multipart
uploads.

It also outputs canonical Bronze, Silver, Gold, rejected, and audit prefixes plus a least-privilege
IAM policy document. Attach that policy to the Databricks instance profile or other runtime identity;
the module does not create users, access keys, or long-lived credentials.

## Validate and review

```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -out=lakehouse.tfplan
terraform show lakehouse.tfplan
```

Review the plan before applying it. `force_destroy` defaults to `false`, preventing accidental
deletion of a bucket that still contains lakehouse data.

```bash
terraform apply lakehouse.tfplan
terraform output layer_uris
```

## State and credentials

Terraform uses the standard AWS credential chain. Configure a short-lived AWS profile, workload
identity, or CI role outside the repository; never add keys to `.tfvars` files. Local state is ignored
by Git, but a team deployment should configure a secured remote backend with encryption, locking,
versioning, and narrowly scoped access before the first shared apply.

CI runs formatting, initialization without a backend, and static validation. It never executes
`terraform plan` or `terraform apply`, so pull requests do not require AWS credentials and cannot
modify infrastructure.

## Application boundary

The current Python pipeline uses local filesystem paths and does not yet write directly to S3. The
Terraform output establishes the target layout without overstating that integration. A production
follow-up should add an object-storage adapter and a transactional table format such as Delta Lake or
Apache Iceberg, then pass the relevant `layer_uris` values into the runtime configuration.
