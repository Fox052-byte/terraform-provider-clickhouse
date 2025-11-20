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
	columnsStatement := ""
	if len(resource.Columns) > 0 {
		columnsList := buildColumnsSentence(resource.GetColumnsResourceList())
		columnsStatement = "(" + strings.Join(columnsList, ", ") + ")"
	}

	clusterStatement := common.GetClusterStatement(resource.Cluster)

	orderBySentence := buildOrderBySentence(resource.OrderBy)
	partitionBySentence := buildPartitionBySentence(resource.PartitionBy)

	createTablePart := fmt.Sprintf("CREATE TABLE %v.%v", resource.Database, resource.Name)
	if clusterStatement != "" {
		createTablePart += " " + clusterStatement
	}
	createTablePart += " " + columnsStatement + " ENGINE = " + resource.Engine + "(" + strings.Join(resource.EngineParams, ", ") + ")"

	parts := []string{createTablePart}

	if orderBySentence != "" {
		parts = append(parts, orderBySentence)
	}

	if partitionBySentence != "" {
		parts = append(parts, partitionBySentence)
	}

	parts = append(parts, fmt.Sprintf("COMMENT '%s'", resource.Comment))

	return strings.Join(parts, " ")
}
