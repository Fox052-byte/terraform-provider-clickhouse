package resourcetable

import (
	"context"
	"fmt"
	"log"

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
	tableResource.Columns = d.Get("column").([]interface{})
	tableResource.Engine = d.Get("engine").(string)
	tableResource.Comment = common.GetComment(d.Get("comment").(string), tableResource.Cluster)
	tableResource.EngineParams = common.MapArrayInterfaceToArrayOfStrings(d.Get("engine_params").([]interface{}))
	
	// order_by - безопасная обработка опционального поля
	orderByRaw := d.Get("order_by")
	log.Printf("DEBUG resourceTableCreate: orderByRaw = %v (type: %T)", orderByRaw, orderByRaw)
	if orderByRaw != nil {
		if orderByList, ok := orderByRaw.([]interface{}); ok {
			log.Printf("DEBUG resourceTableCreate: orderByList = %v", orderByList)
			tableResource.OrderBy = common.MapArrayInterfaceToArrayOfStrings(orderByList)
			log.Printf("DEBUG resourceTableCreate: tableResource.OrderBy = %v", tableResource.OrderBy)
		} else {
			log.Printf("DEBUG resourceTableCreate: orderByRaw is not []interface{}, using empty list")
			tableResource.OrderBy = []string{}
		}
	} else {
		log.Printf("DEBUG resourceTableCreate: orderByRaw is nil, using empty list")
		tableResource.OrderBy = []string{}
	}
	
	// partition_by - безопасная обработка опционального поля
	partitionByRaw := d.Get("partition_by")
	log.Printf("DEBUG resourceTableCreate: partitionByRaw = %v (type: %T)", partitionByRaw, partitionByRaw)
	if partitionByRaw != nil {
		if partitionByList, ok := partitionByRaw.([]interface{}); ok {
			log.Printf("DEBUG resourceTableCreate: partitionByList = %v", partitionByList)
			tableResource.SetPartitionBy(partitionByList)
		} else {
			log.Printf("DEBUG resourceTableCreate: partitionByRaw is not []interface{}, using empty list")
			tableResource.PartitionBy = []PartitionByResource{}
		}
	} else {
		log.Printf("DEBUG resourceTableCreate: partitionByRaw is nil, using empty list")
		tableResource.PartitionBy = []PartitionByResource{}
	}

	if tableResource.Cluster != "" {
		tableResource.Cluster = client.DefaultCluster
	}

	tableResource.Validate(diags)
	if diags.HasError() {
		return diags
	}

	err := chTableService.CreateTable(ctx, tableResource)

	if err != nil {
		return diag.FromErr(err)
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
