locals {
  snowflake_tags = {
    Service-Name = "Snowflake"
  }
}

# S3 read policy
data "aws_iam_policy_document" "snowflake_s3_read" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.spectrum_bucket.arn,
      "${aws_s3_bucket.spectrum_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "snowflake_s3_read" {
  name   = "snowflake-s3-read"
  policy = data.aws_iam_policy_document.snowflake_s3_read.json
}

# IAM Role trust policy for Snowflake
data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::640083578061:user/externalstages/ci445d0000"]  # Snowflake AWS account ID
    }

    actions = ["sts:AssumeRole"]

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["AQ18530_SFCRole=4_m3tuVkfUdAVFgrPX8auu6y5hWpI="]  # External ID from Snowflake
    }
  }
}

# IAM Role for Snowflake to assume
resource "aws_iam_role" "snowflake_role" {
  name               = "SnowflakeReadS3Role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
  tags               = merge(local.generic_tag, local.snowflake_tags)
}

# Attach the S3 read policy to the role
resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_read.arn
}