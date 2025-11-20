package resourcetable

import (
	"strings"
	"testing"
)

func TestBuildCreateOnClusterSentence(t *testing.T) {
	tests := []struct {
		name     string
		resource TableResource
		want     string
	}{
		{
			name: "Table with ORDER BY and COMMENT, no PARTITION BY",
			resource: TableResource{
				Database: "dm",
				Name:     "v_bonus_operations",
				Cluster:  "bi_cluster",
				Engine:   "ReplicatedMergeTree",
				EngineParams: []string{
					"'/clickhouse/tables/{shard}/dm/v_bonus_operations'",
					"'{replica}'",
				},
				OrderBy: []string{"purchase_id", "operation_type"},
				Columns: []interface{}{
					map[string]interface{}{"name": "purchase_id", "type": "UUID"},
					map[string]interface{}{"name": "operation_type", "type": "String"},
					map[string]interface{}{"name": "bonus_amount", "type": "Nullable(Decimal(10, 2))"},
					map[string]interface{}{"name": "wallet_account_id", "type": "Nullable(UUID)"},
					map[string]interface{}{"name": "bonus_date", "type": "Nullable(DateTime64(6))"},
				},
				PartitionBy: []PartitionByResource{},
				Comment:     `{"comment":"","cluster":"bi_cluster"}`,
			},
			want: "CREATE TABLE dm.v_bonus_operations ON CLUSTER bi_cluster (purchase_id UUID, operation_type String, bonus_amount Nullable(Decimal(10, 2)), wallet_account_id Nullable(UUID), bonus_date Nullable(DateTime64(6))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/dm/v_bonus_operations', '{replica}') ORDER BY purchase_id, operation_type COMMENT '{\"comment\":\"\",\"cluster\":\"bi_cluster\"}'",
		},
		{
			name: "Table with ORDER BY, PARTITION BY and COMMENT",
			resource: TableResource{
				Database: "dm",
				Name:     "test_table",
				Cluster:  "bi_cluster",
				Engine:   "ReplicatedMergeTree",
				EngineParams: []string{
					"'/clickhouse/tables/{shard}/dm/test_table'",
					"'{replica}'",
				},
				OrderBy: []string{"date", "id"},
				Columns: []interface{}{
					map[string]interface{}{"name": "id", "type": "Int64"},
					map[string]interface{}{"name": "date", "type": "Date"},
					map[string]interface{}{"name": "value", "type": "String"},
				},
				PartitionBy: []PartitionByResource{
					{By: "date", PartitionFunction: "toYYYYMM"},
				},
				Comment: `{"comment":"test","cluster":"bi_cluster"}`,
			},
			want: "CREATE TABLE dm.test_table ON CLUSTER bi_cluster (id Int64, date Date, value String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/dm/test_table', '{replica}') ORDER BY date, id PARTITION BY toYYYYMM(date) COMMENT '{\"comment\":\"test\",\"cluster\":\"bi_cluster\"}'",
		},
		{
			name: "Table without cluster",
			resource: TableResource{
				Database: "test_db",
				Name:     "simple_table",
				Cluster:  "",
				Engine:   "ReplacingMergeTree",
				EngineParams: []string{"eventTime"},
				OrderBy: []string{"key"},
				Columns: []interface{}{
					map[string]interface{}{"name": "key", "type": "Int64"},
					map[string]interface{}{"name": "value", "type": "String"},
				},
				PartitionBy: []PartitionByResource{},
				Comment:     `{"comment":"simple table","cluster":""}`,
			},
			want: "CREATE TABLE test_db.simple_table  (key Int64, value String) ENGINE = ReplacingMergeTree(eventTime) ORDER BY key COMMENT '{\"comment\":\"simple table\",\"cluster\":\"\"}'",
		},
		{
			name: "Table with all Nullable types from terragrunt.hcl",
			resource: TableResource{
				Database: "dm",
				Name:     "v_bonus_operations",
				Cluster:  "bi_cluster",
				Engine:   "ReplicatedMergeTree",
				EngineParams: []string{
					"'/clickhouse/tables/{shard}/dm/v_bonus_operations'",
					"'{replica}'",
				},
				OrderBy: []string{"purchase_id", "operation_type"},
				Columns: []interface{}{
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
				PartitionBy: []PartitionByResource{},
				Comment:     `{"comment":"","cluster":"bi_cluster"}`,
			},
			want: "CREATE TABLE dm.v_bonus_operations ON CLUSTER bi_cluster (purchase_id UUID, operation_type String, bonus_amount Nullable(Decimal(10, 2)), bonus_expired_amount Nullable(Decimal(10, 2)), wallet_account_id Nullable(UUID), bonus_date Nullable(DateTime64(6)), bonus_activation_date Nullable(DateTime64(6)), bonus_expiration_date Nullable(DateTime64(6)), purchase_operation_id Nullable(UUID)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/dm/v_bonus_operations', '{replica}') ORDER BY purchase_id, operation_type COMMENT '{\"comment\":\"\",\"cluster\":\"bi_cluster\"}'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildCreateOnClusterSentence(tt.resource)

			// Выводим реальный SQL для проверки
			t.Logf("Generated SQL:\n%s\n", got)
			t.Logf("SQL length: %d characters", len(got))

			// Нормализуем пробелы для сравнения
			gotNormalized := strings.Join(strings.Fields(got), " ")
			wantNormalized := strings.Join(strings.Fields(tt.want), " ")

			if gotNormalized != wantNormalized {
				t.Errorf("buildCreateOnClusterSentence() = %v, want %v", got, tt.want)
				t.Errorf("Normalized: got = %v, want = %v", gotNormalized, wantNormalized)
			}

			// Проверяем структуру SQL более детально
			// 1. Проверяем, что нет лишних запятых перед COMMENT
			if strings.Contains(got, ", COMMENT") {
				t.Errorf("ERROR: SQL contains ', COMMENT' which is invalid SQL syntax")
				t.Errorf("SQL: %s", got)
			}

			// 2. Проверяем, что нет запятой перед ORDER BY
			if strings.Contains(got, ", ORDER BY") {
				t.Errorf("ERROR: SQL contains ', ORDER BY' which is invalid")
				t.Errorf("SQL: %s", got)
			}

			// 3. Проверяем, что нет запятой перед PARTITION BY
			if strings.Contains(got, ", PARTITION BY") {
				t.Errorf("ERROR: SQL contains ', PARTITION BY' which is invalid")
				t.Errorf("SQL: %s", got)
			}

			// 4. Проверяем порядок: ORDER BY идет перед COMMENT
			orderByIdx := strings.Index(got, "ORDER BY")
			commentIdx := strings.Index(got, "COMMENT")
			if orderByIdx == -1 || commentIdx == -1 {
				t.Errorf("ERROR: missing ORDER BY or COMMENT")
				t.Errorf("SQL: %s", got)
			}
			if orderByIdx > commentIdx {
				t.Errorf("ERROR: ORDER BY should come before COMMENT")
				t.Errorf("SQL: %s", got)
			}

			// 5. Проверяем, что между ORDER BY и COMMENT нет запятой
			if orderByIdx != -1 && commentIdx != -1 {
				betweenOrderByAndComment := got[orderByIdx:commentIdx]
				// Убираем содержимое ORDER BY (список колонок)
				orderByContent := betweenOrderByAndComment[strings.Index(betweenOrderByAndComment, "ORDER BY")+len("ORDER BY"):]
				orderByContent = strings.TrimSpace(orderByContent)
				// Проверяем, что последний символ перед COMMENT не запятая
				if strings.HasSuffix(orderByContent, ",") {
					t.Errorf("ERROR: ORDER BY clause ends with comma before COMMENT")
					t.Errorf("ORDER BY content: %q", orderByContent)
					t.Errorf("Full SQL: %s", got)
				}
			}

			// 6. Проверяем, что между PARTITION BY и COMMENT нет запятой (если есть PARTITION BY)
			partitionByIdx := strings.Index(got, "PARTITION BY")
			if partitionByIdx != -1 && commentIdx != -1 {
				betweenPartitionByAndComment := got[partitionByIdx:commentIdx]
				partitionByContent := betweenPartitionByAndComment[strings.Index(betweenPartitionByAndComment, "PARTITION BY")+len("PARTITION BY"):]
				partitionByContent = strings.TrimSpace(partitionByContent)
				if strings.HasSuffix(partitionByContent, ",") {
					t.Errorf("ERROR: PARTITION BY clause ends with comma before COMMENT")
					t.Errorf("PARTITION BY content: %q", partitionByContent)
					t.Errorf("Full SQL: %s", got)
				}
			}

			// 7. Проверяем, что нет двойных пробелов (кроме как внутри строк в кавычках)
			// Разбиваем по кавычкам и проверяем части вне кавычек
			parts := strings.Split(got, "'")
			for i, part := range parts {
				if i%2 == 0 { // Части вне кавычек
					if strings.Contains(part, "  ") {
						t.Errorf("ERROR: SQL contains double spaces (outside quotes)")
						t.Errorf("Part: %q", part)
						t.Errorf("Full SQL: %s", got)
					}
				}
			}

			// 8. Проверяем, что нет пробела перед запятой в списке колонок
			if strings.Contains(got, " ,") {
				t.Errorf("ERROR: SQL contains space before comma")
				t.Errorf("SQL: %s", got)
			}

			// 9. Проверяем, что ENGINE параметры правильно оформлены
			engineIdx := strings.Index(got, "ENGINE =")
			if engineIdx != -1 {
				enginePart := got[engineIdx:]
				// Проверяем, что после ENGINE = нет лишних пробелов
				if strings.Contains(enginePart, "ENGINE =  ") {
					t.Errorf("ERROR: SQL contains double space after ENGINE =")
					t.Errorf("SQL: %s", got)
				}
			}
		})
	}
}

