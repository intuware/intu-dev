package auth

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-ldap/ldap/v3"
	"github.com/intuware/intu/pkg/config"
)

type LDAPProvider struct {
	cfg    *config.LDAPConfig
	rbac   *RBACManager
	logger *slog.Logger
}

func NewLDAPProvider(cfg *config.LDAPConfig, rbac *RBACManager, logger *slog.Logger) *LDAPProvider {
	return &LDAPProvider{
		cfg:    cfg,
		rbac:   rbac,
		logger: logger,
	}
}

func (lp *LDAPProvider) Authenticate(r *http.Request) (bool, string, error) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false, "", nil
	}

	if err := lp.validateCredentials(user, pass); err != nil {
		lp.logger.Debug("LDAP authentication failed", "user", user, "error", err)
		return false, "", nil
	}

	return true, user, nil
}

func (lp *LDAPProvider) validateCredentials(username, password string) error {
	conn, err := ldap.DialURL(lp.cfg.URL)
	if err != nil {
		return fmt.Errorf("connect to LDAP: %w", err)
	}
	defer conn.Close()

	if lp.cfg.BindDN != "" && lp.cfg.BindPassword != "" {
		if err := conn.Bind(lp.cfg.BindDN, lp.cfg.BindPassword); err != nil {
			return fmt.Errorf("service account bind: %w", err)
		}
	}

	searchFilter := fmt.Sprintf("(|(uid=%s)(sAMAccountName=%s)(cn=%s))",
		ldap.EscapeFilter(username),
		ldap.EscapeFilter(username),
		ldap.EscapeFilter(username),
	)

	searchRequest := ldap.NewSearchRequest(
		lp.cfg.BaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		1,
		10,
		false,
		searchFilter,
		[]string{"dn", "memberOf", "cn"},
		nil,
	)

	result, err := conn.Search(searchRequest)
	if err != nil {
		return fmt.Errorf("LDAP search: %w", err)
	}

	if len(result.Entries) == 0 {
		return fmt.Errorf("user %s not found in LDAP", username)
	}

	userDN := result.Entries[0].DN

	if err := conn.Bind(userDN, password); err != nil {
		return fmt.Errorf("user bind failed: %w", err)
	}

	lp.logger.Info("LDAP authentication successful", "user", username, "dn", userDN)

	return nil
}

func (lp *LDAPProvider) GetUserGroups(username string) ([]string, error) {
	conn, err := ldap.DialURL(lp.cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("connect to LDAP: %w", err)
	}
	defer conn.Close()

	if lp.cfg.BindDN != "" && lp.cfg.BindPassword != "" {
		if err := conn.Bind(lp.cfg.BindDN, lp.cfg.BindPassword); err != nil {
			return nil, fmt.Errorf("service account bind: %w", err)
		}
	}

	searchFilter := fmt.Sprintf("(|(uid=%s)(sAMAccountName=%s)(cn=%s))",
		ldap.EscapeFilter(username),
		ldap.EscapeFilter(username),
		ldap.EscapeFilter(username),
	)

	searchRequest := ldap.NewSearchRequest(
		lp.cfg.BaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		1,
		10,
		false,
		searchFilter,
		[]string{"dn", "memberOf"},
		nil,
	)

	result, err := conn.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("LDAP search: %w", err)
	}

	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("user not found")
	}

	var groups []string
	for _, attr := range result.Entries[0].GetAttributeValues("memberOf") {
		cn := extractCN(attr)
		if cn != "" {
			groups = append(groups, cn)
		}
	}

	return groups, nil
}

func (lp *LDAPProvider) GetUserRole(username string) (string, error) {
	groups, err := lp.GetUserGroups(username)
	if err != nil {
		return "", err
	}

	if lp.rbac == nil {
		if len(groups) > 0 {
			return groups[0], nil
		}
		return "viewer", nil
	}

	for _, g := range groups {
		if _, err := lp.rbac.GetRole(strings.ToLower(g)); err == nil {
			return strings.ToLower(g), nil
		}
	}

	return "viewer", nil
}

func extractCN(dn string) string {
	for _, part := range strings.Split(dn, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(strings.ToUpper(part), "CN=") {
			return part[3:]
		}
	}
	return ""
}

func NewLDAPAuthMiddleware(provider *LDAPProvider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/api/") {
				next.ServeHTTP(w, r)
				return
			}

			ok, user, err := provider.Authenticate(r)
			if err != nil {
				http.Error(w, "authentication error", http.StatusInternalServerError)
				return
			}
			if !ok {
				w.Header().Set("WWW-Authenticate", `Basic realm="intu dashboard"`)
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}

			r.Header.Set("X-Auth-User", user)
			next.ServeHTTP(w, r)
		})
	}
}
