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

	// КРИТИЧЕСКАЯ проверка
	if strings.Contains(got, ", COMMENT") {
		t.Errorf("❌ CRITICAL BUG: SQL contains ', COMMENT'")
		t.Errorf("Full SQL: %s", got)
		
		idx := strings.Index(got, ", COMMENT")
		t.Errorf("Problem at position %d", idx)
		if idx > 0 {
			start := idx - 100
			if start < 0 {
				start = 0
			}
			end := idx + 50
			if end > len(got) {
				end = len(got)
			}
			t.Errorf("Context: %q", got[start:end])
		}
	} else {
		t.Logf("✅ No ', COMMENT' found - SQL is correct")
	}

	// Проверяем структуру
	orderByIdx := strings.Index(got, "ORDER BY")
	commentIdx := strings.Index(got, "COMMENT")
	
	if orderByIdx == -1 {
		t.Errorf("❌ ORDER BY not found")
	}
	if commentIdx == -1 {
		t.Errorf("❌ COMMENT not found")
	}
	
	if orderByIdx != -1 && commentIdx != -1 {
		if orderByIdx > commentIdx {
			t.Errorf("❌ ORDER BY comes after COMMENT")
		} else {
			t.Logf("✅ ORDER BY comes before COMMENT")
			between := got[orderByIdx:commentIdx]
			t.Logf("Between ORDER BY and COMMENT: %q", between)
			
			// Проверяем, что нет запятой
			if strings.Contains(between, ", COMMENT") || strings.HasSuffix(strings.TrimSpace(between), ",") {
				t.Errorf("❌ Found comma between ORDER BY and COMMENT: %q", between)
			}
		}
	}
}


