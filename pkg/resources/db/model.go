package resourcedb

import "github.com/Fox052-byte/terraform-clickhouse/pkg/resources/table"

type CHDBResources struct {
	CHTables []resourcetable.CHTable
}
