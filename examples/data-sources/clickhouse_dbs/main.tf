terraform {
  required_providers {
    clickhouse = {
      version = "0.0.1"
      source  = "registry.terraform.io/fox052-byte/clickhouse"
    }
  }
}


data "clickhouse_dbs" "this" {}

output "all_dbs" {
  value = data.clickhouse_dbs.this.dbs
}
