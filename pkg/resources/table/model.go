package resourcetable

import (
	"fmt"
	"github.com/Fox052-byte/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"regexp"
)

type CHTable struct {
	Database        string     `ch:"database"`
	Name            string     `ch:"name"`
	EngineFull      string     `ch:"engine_full"`
	Engine          string     `ch:"engine"`
	Comment         string     `ch:"comment"`
	CreateTableQuery string    `ch:"create_table_query"`
	Columns         []CHColumn `ch:"columns"`
}

type CHColumn struct {
	Database string `ch:"database"`
	Table    string `ch:"table"`
	Name     string `ch:"name"`
	Type     string `ch:"type"`
}

type TableResource struct {
	Database     string
	Name         string
	EngineFull   string
	Engine       string
	Cluster      string
	Comment      string
	EngineParams []string
	OrderBy      []string
	Columns      []interface{}
	PartitionBy  []PartitionByResource
}

type ColumnResource struct {
	Name string
	Type string
}

type PartitionByResource struct {
	By                string
	PartitionFunction string
}

func (t *CHTable) ColumnsToResource() []interface{} {
	var columnResources []interface{}
	for _, column := range t.Columns {
		columnResource := map[string]interface{}{
			"name": column.Name,
			"type": column.Type,
		}
		columnResources = append(columnResources, columnResource)
	}

	return columnResources
}

func (t *CHTable) ToResource() (*TableResource, error) {
	tableResource := TableResource{
		Database:   t.Database,
		Name:       t.Name,
		EngineFull: t.EngineFull,
		Engine:     t.Engine,
		Columns:    t.ColumnsToResource(),
	}

	r, _ := regexp.Compile("MergeTree\\((?P<engine_params>[^)]*)\\)")
	matches := r.FindStringSubmatch(t.EngineFull)
	engineParamsIndex := r.SubexpIndex("engine_params")
	engineParams := make([]string, 0)
	if engineParamsIndex != -1 {

		regex := regexp.MustCompile("[, ]+")
		params := regex.Split(matches[r.SubexpIndex("engine_params")], -1)
		for _, p := range params {
			engineParams = append(engineParams, p)
		}
	}

	comment, cluster, err := common.UnmarshalComment(t.Comment)
	if err != nil {
		return nil, err
	}

	tableResource.Cluster = cluster
	tableResource.Comment = comment
	tableResource.EngineParams = engineParams

	createQuery := t.CreateTableQuery
	if createQuery == "" {
		createQuery = t.EngineFull
	}


	orderByRegex := regexp.MustCompile(`(?i)ORDER\s+BY\s+(\([^)]+\)|[^(]+?)(?:\s+PARTITION|\s+COMMENT|\s+SETTINGS|$)`)
	orderByMatches := orderByRegex.FindStringSubmatch(createQuery)
	if len(orderByMatches) > 1 {
		orderByStr := orderByMatches[1]
		orderByStr = regexp.MustCompile(`^\(|\)$`).ReplaceAllString(orderByStr, "")
		orderByStr = regexp.MustCompile(`\s+`).ReplaceAllString(orderByStr, " ")
		orderByStr = regexp.MustCompile(`^\s+|\s+$`).ReplaceAllString(orderByStr, "")
		orderByParts := regexp.MustCompile(`,\s*`).Split(orderByStr, -1)
		tableResource.OrderBy = make([]string, 0)
		for _, part := range orderByParts {
			part = regexp.MustCompile(`^\s+|\s+$`).ReplaceAllString(part, "")
			if part != "" {
				tableResource.OrderBy = append(tableResource.OrderBy, part)
			}
		}
	}


	partitionByRegex := regexp.MustCompile(`(?i)PARTITION\s+BY\s+((?:[^\s]|\([^)]+\))+?)(?:\s+(?:ORDER|COMMENT|SETTINGS)|\s*$)`)
	partitionByMatches := partitionByRegex.FindStringSubmatch(createQuery)
	if len(partitionByMatches) > 1 {
		partitionByStr := partitionByMatches[1]
		partitionByStr = regexp.MustCompile(`\s+`).ReplaceAllString(partitionByStr, " ")
		partitionByStr = regexp.MustCompile(`^\s+|\s+$`).ReplaceAllString(partitionByStr, "")
		
		funcRegex := regexp.MustCompile(`^(toYYYYMM|toYYYYMMDD|toYYYYMMDDhhmmss)\(([^)]+)\)$`)
		funcMatches := funcRegex.FindStringSubmatch(partitionByStr)
		if len(funcMatches) > 2 {
			tableResource.PartitionBy = []PartitionByResource{
				{
					By:                funcMatches[2],
					PartitionFunction: funcMatches[1],
				},
			}
		} else {
			tableResource.PartitionBy = []PartitionByResource{
				{
					By:                partitionByStr,
					PartitionFunction: "",
				},
			}
		}
	}

	return &tableResource, nil
}

func (t *TableResource) GetColumnsResourceList() []ColumnResource {
	var columnResources []ColumnResource
	for _, column := range t.Columns {
		columnResources = append(columnResources, ColumnResource{
			Name: column.(map[string]interface{})["name"].(string),
			Type: column.(map[string]interface{})["type"].(string),
		})
	}
	return columnResources
}

func (t *TableResource) SetPartitionBy(partitionBy []interface{}) {
	for _, partitionBy := range partitionBy {
		partitionByResource := PartitionByResource{
			By:                partitionBy.(map[string]interface{})["by"].(string),
			PartitionFunction: partitionBy.(map[string]interface{})["partition_function"].(string),
		}
		t.PartitionBy = append(t.PartitionBy, partitionByResource)
	}
}

func (t *TableResource) HasColumn(columnName string) bool {
	for _, column := range t.GetColumnsResourceList() {
		if column.Name == columnName {
			return true
		}
	}
	return false
}

func (t *TableResource) Validate(diags diag.Diagnostics) {
	t.validateOrderBy(diags)
	t.validatePartitionBy(diags)
}

func (t *TableResource) validateOrderBy(diags diag.Diagnostics) {
	for _, orderField := range t.OrderBy {
		if t.HasColumn(orderField) == false {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "wrong value",
				Detail:   fmt.Sprintf("order by field '%s' is not a column", orderField),
			})
		}
	}
}

func (t *TableResource) validatePartitionBy(diags diag.Diagnostics) {
	for _, partitionBy := range t.PartitionBy {
		if t.HasColumn(partitionBy.By) == false {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "wrong value",
				Detail:   fmt.Sprintf("partition by field '%s' is not a column", partitionBy.By),
			})
		}
	}
}
