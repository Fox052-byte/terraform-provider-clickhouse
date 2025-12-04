package resourcetable

import (
	"fmt"
	"strings"

	v "github.com/go-playground/validator/v10"
	hashicorpcty "github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
)

func ValidatePartitionBy(inValue any, p hashicorpcty.Path) diag.Diagnostics {
	validate := v.New()
	value := inValue.(string)
	toAllowedPartitioningFunctions := "toYYYYMM toYYYYMMDD toYYYYMMDDhhmmss"
	validation := fmt.Sprintf("oneof=%v", toAllowedPartitioningFunctions)
	var diags diag.Diagnostics
	if validate.Var(value, validation) != nil {
		diag := diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "wrong value",
			Detail:   fmt.Sprintf("%q is not %q", value, toAllowedPartitioningFunctions),
		}
		diags = append(diags, diag)
	}
	return diags
}

func ValidateType(inValue any, p hashicorpcty.Path) diag.Diagnostics {
	value := inValue.(string)
	baseType := value
	if len(value) > 9 && value[:9] == "Nullable(" {
		parenCount := 0
		endIdx := -1
		// Ищем закрывающую скобку для Nullable, учитывая вложенные скобки
		for i := 9; i < len(value); i++ {
			if value[i] == '(' {
				parenCount++
			} else if value[i] == ')' {
				if parenCount == 0 {
					// Нашли закрывающую скобку для Nullable
					endIdx = i
					break
				}
				parenCount--
			}
		}
		if endIdx >= 0 {
			baseType = value[9:endIdx]
		}
	}
	simpleType := baseType
	// Извлекаем базовый тип, убирая параметры в скобках (например, Decimal(10, 2) -> Decimal)
	if idx := strings.Index(baseType, "("); idx > 0 {
		simpleType = baseType[:idx]
	}

	allowedTypes := []string{
		"UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
		"Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
		"Float32", "Float64",
		"Bool", "String", "UUID", "Date", "Date32", "DateTime", "DateTime64",
		"LowCardinality", "JSON", "Decimal",
	}

	found := false
	for _, allowedType := range allowedTypes {
		if simpleType == allowedType {
			found = true
			break
		}
	}

	var diags diag.Diagnostics
	if !found {
		diag := diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "wrong value",
			Detail:   fmt.Sprintf("%q is not a valid type. Allowed types: %v", value, allowedTypes),
		}
		diags = append(diags, diag)
	}
	return diags
}

func ValidateOnClusterEngine(inValue any, p hashicorpcty.Path) diag.Diagnostics {
	validate := v.New()
	value := inValue.(string)
	mergeTreeTypes := "ReplacingMergeTree"
	replicatedTypes := "ReplicatedMergeTree"
	distributedTypes := "Distributed"
	validation := fmt.Sprintf("oneof=%v %v %v", replicatedTypes, distributedTypes, mergeTreeTypes)
	var diags diag.Diagnostics
	if validate.Var(value, validation) != nil {
		diag := diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "wrong value",
			Detail:   fmt.Sprintf("%q is not %q %q %q", value, replicatedTypes, distributedTypes, mergeTreeTypes),
		}
		diags = append(diags, diag)
	}
	return diags
}
