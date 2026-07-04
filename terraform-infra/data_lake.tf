locals {
  service = {
    Service-Name = "Airflow"
  }
}


resource "aws_s3_bucket" "spectrum_bucket" {
  bucket = "supplychain360-redshift-spectrum"

  tags = merge(
    local.service,
    local.generic_tag
  )
}


resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.spectrum_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}