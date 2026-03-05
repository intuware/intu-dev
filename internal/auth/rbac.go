package auth

import (
	"fmt"
	"strings"

	"github.com/intuware/intu/pkg/config"
)

type RBACManager struct {
	roles map[string]*Role
}

type Role struct {
	Name        string
	Permissions map[string]bool
}

func NewRBACManager(roles []config.RoleConfig) *RBACManager {
	rm := &RBACManager{roles: make(map[string]*Role)}
	for _, rc := range roles {
		role := &Role{
			Name:        rc.Name,
			Permissions: make(map[string]bool),
		}
		for _, p := range rc.Permissions {
			role.Permissions[p] = true
		}
		rm.roles[rc.Name] = role
	}
	return rm
}

func (rm *RBACManager) HasPermission(roleName, permission string) bool {
	role, ok := rm.roles[roleName]
	if !ok {
		return false
	}

	if role.Permissions["*"] {
		return true
	}

	if role.Permissions[permission] {
		return true
	}

	parts := strings.Split(permission, ".")
	if len(parts) > 1 {
		wildcard := parts[0] + ".*"
		if role.Permissions[wildcard] {
			return true
		}
	}

	return false
}

func (rm *RBACManager) GetRole(name string) (*Role, error) {
	role, ok := rm.roles[name]
	if !ok {
		return nil, fmt.Errorf("role %q not found", name)
	}
	return role, nil
}

func (rm *RBACManager) ListRoles() []string {
	var names []string
	for name := range rm.roles {
		names = append(names, name)
	}
	return names
}
