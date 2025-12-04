package resourcetable

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/Fox052-byte/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourcePostgreSQLTable() *schema.Resource {
	return &schema.Resource{
		Description: "Resource to manage PostgreSQL engine tables in ClickHouse",

		CreateContext: resourcePostgreSQLTableCreate,
		ReadContext:   resourcePostgreSQLTableRead,
		UpdateContext: resourcePostgreSQLTableUpdate,
		DeleteContext: resourcePostgreSQLTableDelete,
		Schema: map[string]*schema.Schema{
			"database": {
				Description: "DB Name where the table will be created",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			"comment": {
				Description: "Table comment",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    false,
			},
			"name": {
				Description: "Table Name",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			"engine_params": {
				Description: "PostgreSQL engine params: [host:port, database, table, user, password, schema]",
				Type:        schema.TypeList,
				Required:    true,
				ForceNew:    true,
				Elem: &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
				},
			},
			"column": {
				Description: "Column",
				Type:        schema.TypeList,
				Required:    true,
				ForceNew:    true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Description: "Column Name",
							Type:        schema.TypeString,
							Required:    true,
							ForceNew:    true,
						},
						"type": {
							Description:      "Column Type",
							Type:             schema.TypeString,
							Required:         true,
							ValidateDiagFunc: ValidateType,
							ForceNew:         true,
						},
					},
				},
			},
		},
	}
}

func resourcePostgreSQLTableRead(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	var diags diag.Diagnostics

	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection

	database := d.Get("database").(string)
	tableName := d.Get("name").(string)

	chTableService := CHTableService{CHConnection: conn}
	chTable, err := chTableService.GetTable(ctx, database, tableName)

	if err != nil {
		return diag.FromErr(fmt.Errorf("reading Clickhouse PostgreSQL table: %v", err))
	}

	tableResource, err := chTable.ToPostgreSQLResource()
	if err != nil {
		return diag.FromErr(fmt.Errorf("transforming Clickhouse table to PostgreSQL resource: %v", err))
	}

	if err := d.Set("database", tableResource.Database); err != nil {
		return diag.FromErr(fmt.Errorf("setting database: %v", err))
	}
	if err := d.Set("name", tableResource.Name); err != nil {
		return diag.FromErr(fmt.Errorf("setting name: %v", err))
	}
	if err := d.Set("engine_params", tableResource.EngineParams); err != nil {
		return diag.FromErr(fmt.Errorf("setting engine_params: %v", err))
	}
	if err := d.Set("column", tableResource.Columns); err != nil {
		return diag.FromErr(fmt.Errorf("setting column: %v", err))
	}
	if err := d.Set("comment", tableResource.Comment); err != nil {
		return diag.FromErr(fmt.Errorf("setting comment: %v", err))
	}

	d.SetId(database + ":" + tableName)

	return diags
}

func resourcePostgreSQLTableCreate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	var diags diag.Diagnostics

	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection
	tableResource := PostgreSQLTableResource{}
	chTableService := CHTableService{CHConnection: conn}

	tableResource.Database = d.Get("database").(string)
	tableResource.Name = d.Get("name").(string)

	commentRaw := d.Get("comment")
	commentStr := ""
	if commentRaw != nil {
		commentStr = commentRaw.(string)
	}
	tableResource.Comment = commentStr

	columnRaw := d.Get("column")
	if columnRaw != nil {
		if columnList, ok := columnRaw.([]interface{}); ok {
			tableResource.Columns = columnList
		} else {
			tableResource.Columns = []interface{}{}
		}
	} else {
		tableResource.Columns = []interface{}{}
	}

	engineParamsRaw := d.Get("engine_params")
	if engineParamsRaw != nil {
		if engineParamsList, ok := engineParamsRaw.([]interface{}); ok {
			tableResource.EngineParams = common.MapArrayInterfaceToArrayOfStrings(engineParamsList)
		} else {
			tableResource.EngineParams = []string{}
		}
	} else {
		tableResource.EngineParams = []string{}
	}

	query := buildCreatePostgreSQLTableSentence(tableResource)
	err := chTableService.CreatePostgreSQLTable(ctx, tableResource, commentStr)

	if err != nil {
		return diag.FromErr(fmt.Errorf("creating PostgreSQL table failed. SQL: %s, error: %v", query, err))
	}

	d.SetId(tableResource.Database + ":" + tableResource.Name)

	return diags
}

func resourcePostgreSQLTableUpdate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection
	chTableService := CHTableService{CHConnection: conn}
	if d.HasChange("comment") {
		tableResource := PostgreSQLTableResource{}
		tableResource.Database = d.Get("database").(string)
		tableResource.Name = d.Get("name").(string)

		commentRaw := d.Get("comment")
		commentStr := ""
		if commentRaw != nil {
			commentStr = commentRaw.(string)
		}
		tableResource.Comment = commentStr

		err := chTableService.UpdatePostgreSQLTableComment(ctx, tableResource, commentStr)
		if err != nil {
			return diag.FromErr(fmt.Errorf("updating PostgreSQL table comment: %v", err))
		}
	}
	return resourcePostgreSQLTableRead(ctx, d, meta)
}

func resourcePostgreSQLTableDelete(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	var diags diag.Diagnostics
	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection
	chTableService := CHTableService{CHConnection: conn}

	var tableResource PostgreSQLTableResource
	tableResource.Database = d.Get("database").(string)
	tableResource.Name = d.Get("name").(string)

	err := chTableService.DeletePostgreSQLTable(ctx, tableResource)

	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}

func buildCreatePostgreSQLTableSentence(resource PostgreSQLTableResource) (query string) {
	parts := []string{fmt.Sprintf("CREATE TABLE %s.%s", resource.Database, resource.Name)}

	if len(resource.Columns) > 0 {
		columnsList := buildColumnsSentence(resource.GetColumnsResourceList())
		parts = append(parts, "("+strings.Join(columnsList, ", ")+")")
	}

	if len(resource.EngineParams) > 0 {
		engineParamsStr := strings.Join(resource.EngineParams, ", ")
		parts = append(parts, fmt.Sprintf("ENGINE = PostgreSQL(%s)", engineParamsStr))
	} else {
		parts = append(parts, "ENGINE = PostgreSQL")
	}

	return strings.Join(parts, " ")
}

type PostgreSQLTableResource struct {
	Database     string
	Name         string
	Comment      string
	EngineParams []string
	Columns      []interface{}
}

func (t *PostgreSQLTableResource) GetColumnsResourceList() []ColumnResource {
	var columnResources []ColumnResource
	for _, column := range t.Columns {
		columnResources = append(columnResources, ColumnResource{
			Name: column.(map[string]interface{})["name"].(string),
			Type: column.(map[string]interface{})["type"].(string),
		})
	}
	return columnResources
}

func (t *CHTable) ToPostgreSQLResource() (*PostgreSQLTableResource, error) {
	if t.Engine != "PostgreSQL" {
		return nil, fmt.Errorf("table engine is not PostgreSQL, got: %s", t.Engine)
	}

	tableResource := PostgreSQLTableResource{
		Database: t.Database,
		Name:     t.Name,
		Columns:  t.ColumnsToResource(),
	}

	//  PostgreSQL('host:port', 'database', 'table', 'user', 'password', 'schema')
	postgresqlRegex := regexp.MustCompile(`PostgreSQL\(([^)]+)\)`)
	matches := postgresqlRegex.FindStringSubmatch(t.EngineFull)
	if len(matches) > 1 {
		paramsStr := matches[1]
		params := parsePostgreSQLParams(paramsStr)
		tableResource.EngineParams = params
	}

	comment, _, err := common.UnmarshalComment(t.Comment)
	if err != nil {
		return nil, err
	}
	tableResource.Comment = comment

	return &tableResource, nil
}

func parsePostgreSQLParams(paramsStr string) []string {
	var params []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(paramsStr); i++ {
		char := paramsStr[i]

		if (char == '\'' || char == '"') && (i == 0 || paramsStr[i-1] != '\\') {
			if !inQuotes {
				inQuotes = true
				quoteChar = char
			} else if char == quoteChar {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(char)
		} else if char == ',' && !inQuotes {
			param := strings.TrimSpace(current.String())
			if param != "" {
				params = append(params, param)
			}
			current.Reset()
		} else {
			current.WriteByte(char)
		}
	}

	param := strings.TrimSpace(current.String())
	if param != "" {
		params = append(params, param)
	}

	return params
}
