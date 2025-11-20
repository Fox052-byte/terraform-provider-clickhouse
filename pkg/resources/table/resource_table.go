package resourcetable

import (
	"context"
	"fmt"

	"github.com/Fox052-byte/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourceTable() *schema.Resource {
	return &schema.Resource{
		Description: "Resource to manage tables",

		CreateContext: resourceTableCreate,
		ReadContext:   resourceTableRead,
		DeleteContext: resourceTableDelete,
		Schema: map[string]*schema.Schema{
			"database": {
				Description: "DB Name where the table will bellow",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			"comment": {
				Description: "Database comment, it will be codified in a json along with come metadata information (like cluster name in case of clustering)",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
			"name": {
				Description: "Table Name",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			"cluster": {
				Description: "Cluster Name, it is required for Replicated or Distributed tables and forbidden in other case",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
			"engine": {
				Description:      "Table engine type (Supported types so far: Distributed, ReplicatedReplacingMergeTree, ReplacingMergeTree)",
				Type:             schema.TypeString,
				Required:         true,
				ForceNew:         true,
				ValidateDiagFunc: ValidateOnClusterEngine,
			},
			"engine_params": {
				Description: "Engine params in case the engine type requires them",
				Type:        schema.TypeList,
				Required:    true,
				ForceNew:    true,
				Elem: &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
				},
			},
			"order_by": {
				Description: "Order by columns to use as sorting key",
				Type:        schema.TypeList,
				Optional:    true,
				ForceNew:    true,
				Elem: &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
				},
			},
			"partition_by": {
				Description: "Partition Key to split data",
				Type:        schema.TypeList,
				Optional:    true,
				ForceNew:    true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"by": {
							Description: "Column to use as part of the partition key",
							Type:        schema.TypeString,
							Required:    true,
							ForceNew:    true,
						},
						"partition_function": {
							Description:      "Partition function, could be empty or one of following: toYYYYMM, toYYYYMMDD or toYYYYMMDDhhmmss",
							Type:             schema.TypeString,
							Optional:         true,
							ValidateDiagFunc: ValidatePartitionBy,
							Default:          nil,
							ForceNew:         true,
						},
					},
				},
			},
			"column": {
				Description: "Column",
				Type:        schema.TypeList,
				Optional:    true,
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

func resourceTableRead(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	var diags diag.Diagnostics

	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection

	database := d.Get("database").(string)
	tableName := d.Get("name").(string)

	chTableService := CHTableService{CHConnection: conn}
	chTable, err := chTableService.GetTable(ctx, database, tableName)

	if err != nil {
		return diag.FromErr(fmt.Errorf("reading Clickhouse table: %v", err))
	}

	tableResource, err := chTable.ToResource()
	if err != nil {
		return diag.FromErr(fmt.Errorf("transforming Clickhouse table to resource: %v", err))
	}

	if err := d.Set("database", tableResource.Database); err != nil {
		return diag.FromErr(fmt.Errorf("setting database: %v", err))
	}
	if err := d.Set("name", tableResource.Name); err != nil {
		return diag.FromErr(fmt.Errorf("setting name: %v", err))
	}
	if err := d.Set("engine", tableResource.Engine); err != nil {
		return diag.FromErr(fmt.Errorf("setting engine: %v", err))
	}
	if err := d.Set("engine_params", tableResource.EngineParams); err != nil {
		return diag.FromErr(fmt.Errorf("setting engine_params: %v", err))
	}
	if err := d.Set("cluster", tableResource.Cluster); err != nil {
		return diag.FromErr(fmt.Errorf("setting cluster: %v", err))
	}
	if err := d.Set("order_by", tableResource.OrderBy); err != nil {
		return diag.FromErr(fmt.Errorf("setting order_by: %v", err))
	}
	// Преобразуем PartitionByResource в формат для Terraform schema
	partitionByList := make([]interface{}, 0)
	for _, pb := range tableResource.PartitionBy {
		partitionByMap := map[string]interface{}{
			"by": pb.By,
		}
		if pb.PartitionFunction != "" {
			partitionByMap["partition_function"] = pb.PartitionFunction
		}
		partitionByList = append(partitionByList, partitionByMap)
	}
	if err := d.Set("partition_by", partitionByList); err != nil {
		return diag.FromErr(fmt.Errorf("setting partition_by: %v", err))
	}
	if err := d.Set("column", tableResource.Columns); err != nil {
		return diag.FromErr(fmt.Errorf("setting column: %v", err))
	}

	d.SetId(tableResource.Cluster + ":" + database + ":" + tableName)

	return diags
}

func resourceTableCreate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	var diags diag.Diagnostics

	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection
	tableResource := TableResource{}
	chTableService := CHTableService{CHConnection: conn}

	tableResource.Cluster = d.Get("cluster").(string)
	tableResource.Database = d.Get("database").(string)
	tableResource.Name = d.Get("name").(string)
	tableResource.Engine = d.Get("engine").(string)
	
	// Безопасная обработка опциональных полей
	commentRaw := d.Get("comment")
	commentStr := ""
	if commentRaw != nil {
		commentStr = commentRaw.(string)
	}
	tableResource.Comment = common.GetComment(commentStr, tableResource.Cluster)
	
	// Обработка columns
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
	
	// Обработка engine_params
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
	
	// order_by - упрощенная обработка опционального поля
	orderByRaw := d.Get("order_by")
	if orderByRaw != nil {
		if orderByList, ok := orderByRaw.([]interface{}); ok && len(orderByList) > 0 {
			tableResource.OrderBy = common.MapArrayInterfaceToArrayOfStrings(orderByList)
		} else {
			tableResource.OrderBy = []string{}
		}
	} else {
		tableResource.OrderBy = []string{}
	}
	
	// partition_by - безопасная обработка опционального поля
	partitionByRaw := d.Get("partition_by")
	if partitionByRaw != nil {
		if partitionByList, ok := partitionByRaw.([]interface{}); ok {
			tableResource.SetPartitionBy(partitionByList)
		} else {
			tableResource.PartitionBy = []PartitionByResource{}
		}
	} else {
		tableResource.PartitionBy = []PartitionByResource{}
	}

	if tableResource.Cluster == "" {
		tableResource.Cluster = client.DefaultCluster
	}

	tableResource.Validate(diags)
	if diags.HasError() {
		return diags
	}

	// Формируем SQL запрос
	query := buildCreateOnClusterSentence(tableResource)
	err := chTableService.CreateTable(ctx, tableResource)

	if err != nil {
		// В случае ошибки включаем SQL запрос в сообщение для отладки
		return diag.FromErr(fmt.Errorf("creating table failed. SQL: %s, error: %v", query, err))
	}

	d.SetId(tableResource.Cluster + ":" + tableResource.Database + ":" + tableResource.Name)

	return diags
}

func resourceTableDelete(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	var diags diag.Diagnostics
	client := meta.(*common.ApiClient)
	conn := client.ClickhouseConnection
	chTableService := CHTableService{CHConnection: conn}

	var tableResource TableResource
	tableResource.Database = d.Get("database").(string)
	tableResource.Name = d.Get("name").(string)
	tableResource.Cluster = d.Get("cluster").(string)
	if tableResource.Cluster == "" {
		tableResource.Cluster = client.DefaultCluster
	}

	err := chTableService.DeleteTable(ctx, tableResource)

	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}
