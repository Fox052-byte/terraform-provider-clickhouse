package resourcetable

import (
	"fmt"
	"github.com/Fox052-byte/terraform-provider-clickhouse/pkg/common"
	"strings"
)

func buildColumnsSentence(cols []ColumnResource) []string {
	outColumn := make([]string, 0)
	for _, col := range cols {
		outColumn = append(outColumn, fmt.Sprintf("%s %s", col.Name, col.Type))
	}
	return outColumn
}

func buildPartitionBySentence(partitionBy []PartitionByResource) string {
	if len(partitionBy) > 0 {
		partitionBySentenceItems := make([]string, 0)
		for _, partitionByItem := range partitionBy {
			if partitionByItem.PartitionFunction == "" {
				partitionBySentenceItems = append(partitionBySentenceItems, partitionByItem.By)
			} else {
				partitionBySentenceItems = append(partitionBySentenceItems, fmt.Sprintf("%v(%v)", partitionByItem.PartitionFunction, partitionByItem.By))
			}
		}
		return fmt.Sprintf("PARTITION BY %v", strings.Join(partitionBySentenceItems, ", "))
	}
	return ""
}

func buildOrderBySentence(orderBy []string) string {
	if len(orderBy) > 0 {
		return fmt.Sprintf("ORDER BY %v", strings.Join(orderBy, ", "))
	}
	return ""
}

func buildCreateOnClusterSentence(resource TableResource) (query string) {
	// Формируем базовую часть CREATE TABLE
	parts := []string{fmt.Sprintf("CREATE TABLE %s.%s", resource.Database, resource.Name)}
	
	// Добавляем ON CLUSTER если указан
	if resource.Cluster != "" {
		parts = append(parts, common.GetClusterStatement(resource.Cluster))
	}
	
	// Добавляем колонки
	if len(resource.Columns) > 0 {
		columnsList := buildColumnsSentence(resource.GetColumnsResourceList())
		parts = append(parts, "("+strings.Join(columnsList, ", ")+")")
	}
	
	// Добавляем ENGINE
	engineParamsStr := strings.Join(resource.EngineParams, ", ")
	parts = append(parts, fmt.Sprintf("ENGINE = %s(%s)", resource.Engine, engineParamsStr))
	
	// Добавляем ORDER BY если указан
	if len(resource.OrderBy) > 0 {
		parts = append(parts, buildOrderBySentence(resource.OrderBy))
	}
	
	// Добавляем PARTITION BY если указан
	if len(resource.PartitionBy) > 0 {
		parts = append(parts, buildPartitionBySentence(resource.PartitionBy))
	}

	return strings.Join(parts, " ")
}
