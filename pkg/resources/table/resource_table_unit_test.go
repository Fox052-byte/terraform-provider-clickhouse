package resourcetable

import (
	"strings"
	"testing"

	"github.com/Fox052-byte/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Тест, который симулирует реальный вызов через terraform SDK
func TestResourceTableCreate_ExactTerragruntData(t *testing.T) {
	// Симулируем данные из terragrunt.hcl
	resourceData := schema.TestResourceDataRaw(t, ResourceTable().Schema, map[string]interface{}{
		"name":     "v_bonus_operations",
		"database": "dm",
		"engine":   "ReplicatedMergeTree",
		"cluster":  "bi_cluster",
		"comment":  "",
		"engine_params": []interface{}{
			"'/clickhouse/tables/{shard}/dm/v_bonus_operations'",
			"'{replica}'",
		},
		"order_by": []interface{}{
			"purchase_id",
			"operation_type",
		},
		"column": []interface{}{
			map[string]interface{}{"name": "purchase_id", "type": "UUID"},
			map[string]interface{}{"name": "operation_type", "type": "String"},
			map[string]interface{}{"name": "bonus_amount", "type": "Nullable(Decimal(10, 2))"},
			map[string]interface{}{"name": "bonus_expired_amount", "type": "Nullable(Decimal(10, 2))"},
			map[string]interface{}{"name": "wallet_account_id", "type": "Nullable(UUID)"},
			map[string]interface{}{"name": "bonus_date", "type": "Nullable(DateTime64(6))"},
			map[string]interface{}{"name": "bonus_activation_date", "type": "Nullable(DateTime64(6))"},
			map[string]interface{}{"name": "bonus_expiration_date", "type": "Nullable(DateTime64(6))"},
			map[string]interface{}{"name": "purchase_operation_id", "type": "Nullable(UUID)"},
		},
		// partition_by не указан в terragrunt.hcl, поэтому nil
		"partition_by": nil,
	})

	// Создаем TableResource так же, как это делает resourceTableCreate
	tableResource := TableResource{}
	tableResource.Cluster = resourceData.Get("cluster").(string)
	tableResource.Database = resourceData.Get("database").(string)
	tableResource.Name = resourceData.Get("name").(string)
	tableResource.Columns = resourceData.Get("column").([]interface{})
	tableResource.Engine = resourceData.Get("engine").(string)
	tableResource.Comment = common.GetComment(resourceData.Get("comment").(string), tableResource.Cluster)
	tableResource.EngineParams = common.MapArrayInterfaceToArrayOfStrings(resourceData.Get("engine_params").([]interface{}))

	// Обрабатываем order_by
	orderByRaw := resourceData.Get("order_by")
	if orderByRaw != nil {
		tableResource.OrderBy = common.MapArrayInterfaceToArrayOfStrings(orderByRaw.([]interface{}))
	} else {
		tableResource.OrderBy = []string{}
	}

	// Обрабатываем partition_by
	partitionByRaw := resourceData.Get("partition_by")
	if partitionByRaw != nil {
		tableResource.SetPartitionBy(partitionByRaw.([]interface{}))
	} else {
		tableResource.PartitionBy = []PartitionByResource{}
	}

	t.Logf("=== TableResource from Terraform SDK ===")
	t.Logf("OrderBy: %v", tableResource.OrderBy)
	t.Logf("PartitionBy: %v", tableResource.PartitionBy)
	t.Logf("Columns count: %d", len(tableResource.Columns))

	// Генерируем SQL
	got := buildCreateOnClusterSentence(tableResource)

	t.Logf("=== Generated SQL ===")
	t.Logf("SQL: %s", got)
	t.Logf("SQL length: %d", len(got))

	// Проверяем структуру SQL
	orderByIdx := strings.Index(got, "ORDER BY")
	
	if orderByIdx == -1 {
		t.Errorf("❌ ORDER BY not found")
	} else {
		t.Logf("✅ ORDER BY found at position %d", orderByIdx)
	}
	
	// Проверяем, что ORDER BY корректно сформирован
	if orderByIdx != -1 {
		orderByPart := got[orderByIdx:]
		t.Logf("ORDER BY part: %q", orderByPart)
		
		// Проверяем, что после ORDER BY нет лишних запятых
		if strings.Contains(orderByPart, ",,") {
			t.Errorf("❌ Found double comma in ORDER BY: %q", orderByPart)
		}
	}
}


