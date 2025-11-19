terraform {
  required_providers {
    clickhouse = {
      version = "0.0.1"
      source  = "registry.terraform.io/fox052-byte/clickhouse"
    }
  }
}


provider "clickhouse" {
  port           = 8123
  clickhouse_url = "127.0.0.1"
  username       = "default"
  password       = ""
}
