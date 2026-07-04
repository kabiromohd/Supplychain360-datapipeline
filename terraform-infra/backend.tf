terraform {
  backend "s3" {
    bucket       = "supplychain360-state"
    key          = "dev/dev.tfstate"
    use_lockfile = true
    region       = "eu-north-1"
  }
}
