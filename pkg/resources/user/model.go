package resourceuser

import (
	"github.com/Fox052-byte/terraform-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type CHUser struct {
	Name  string   `ch:"name"`
	Roles []string `ch:"default_roles_list"`
}

type UserResource struct {
	Name     string
	Password string
	Roles    *schema.Set
}

func (u *CHUser) ToUserResource() *UserResource {
	return &UserResource{
		Name:  u.Name,
		Roles: common.StringListToSet(u.Roles),
	}
}
