package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
	ErrInvalidClaims = errors.New("invalid claims")
)

// JWTManager manages JWT tokens
type JWTManager struct {
	secretKey     string
	tokenDuration time.Duration
}

// UserClaims represents JWT claims
type UserClaims struct {
	UserID    string   `json:"user_id"`
	Username  string   `json:"username"`
	Roles     []string `json:"roles"`
	Namespace string   `json:"namespace"`
	jwt.RegisteredClaims
}

// NewJWTManager creates a new JWT manager
func NewJWTManager(secretKey string, tokenDuration time.Duration) *JWTManager {
	return &JWTManager{
		secretKey:     secretKey,
		tokenDuration: tokenDuration,
	}
}

// GenerateToken generates a new JWT token
func (m *JWTManager) GenerateToken(userID, username, namespace string, roles []string) (string, error) {
	claims := UserClaims{
		UserID:    userID,
		Username:  username,
		Roles:     roles,
		Namespace: namespace,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(m.tokenDuration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(m.secretKey))
}

// ValidateToken validates a JWT token
func (m *JWTManager) ValidateToken(tokenString string) (*UserClaims, error) {
	token, err := jwt.ParseWithClaims(
		tokenString,
		&UserClaims{},
		func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(m.secretKey), nil
		},
	)

	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*UserClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	if time.Now().After(claims.ExpiresAt.Time) {
		return nil, ErrExpiredToken
	}

	return claims, nil
}

// RefreshToken generates a new token with updated expiration
func (m *JWTManager) RefreshToken(oldToken string) (string, error) {
	claims, err := m.ValidateToken(oldToken)
	if err != nil {
		return "", err
	}

	return m.GenerateToken(claims.UserID, claims.Username, claims.Namespace, claims.Roles)
}

// Permission represents a permission type
type Permission string

const (
	PermissionCreateTask   Permission = "task:create"
	PermissionReadTask     Permission = "task:read"
	PermissionUpdateTask   Permission = "task:update"
	PermissionDeleteTask   Permission = "task:delete"
	PermissionManageWorkers Permission = "worker:manage"
	PermissionViewMetrics  Permission = "metrics:view"
	PermissionAdmin        Permission = "admin:all"
)

// Role represents a user role with permissions
type Role struct {
	Name        string
	Permissions []Permission
}

var (
	RoleAdmin = Role{
		Name: "admin",
		Permissions: []Permission{
			PermissionAdmin,
			PermissionCreateTask,
			PermissionReadTask,
			PermissionUpdateTask,
			PermissionDeleteTask,
			PermissionManageWorkers,
			PermissionViewMetrics,
		},
	}

	RoleOperator = Role{
		Name: "operator",
		Permissions: []Permission{
			PermissionCreateTask,
			PermissionReadTask,
			PermissionUpdateTask,
			PermissionDeleteTask,
			PermissionViewMetrics,
		},
	}

	RoleViewer = Role{
		Name: "viewer",
		Permissions: []Permission{
			PermissionReadTask,
			PermissionViewMetrics,
		},
	}
)

// RoleManager manages user roles and permissions
type RoleManager struct {
	roles map[string]*Role
}

// NewRoleManager creates a new role manager
func NewRoleManager() *RoleManager {
	rm := &RoleManager{
		roles: make(map[string]*Role),
	}

	// Register default roles
	rm.roles[RoleAdmin.Name] = &RoleAdmin
	rm.roles[RoleOperator.Name] = &RoleOperator
	rm.roles[RoleViewer.Name] = &RoleViewer

	return rm
}

// HasPermission checks if roles have a specific permission
func (rm *RoleManager) HasPermission(roleNames []string, permission Permission) bool {
	for _, roleName := range roleNames {
		role, exists := rm.roles[roleName]
		if !exists {
			continue
		}

		// Admin has all permissions
		for _, p := range role.Permissions {
			if p == PermissionAdmin || p == permission {
				return true
			}
		}
	}

	return false
}

// APIKey represents an API key for authentication
type APIKey struct {
	Key       string
	UserID    string
	Namespace string
	Roles     []string
	CreatedAt time.Time
	ExpiresAt *time.Time
	Active    bool
}

// GenerateAPIKey generates a random API key
func GenerateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// APIKeyManager manages API keys
type APIKeyManager struct {
	keys map[string]*APIKey
}

// NewAPIKeyManager creates a new API key manager
func NewAPIKeyManager() *APIKeyManager {
	return &APIKeyManager{
		keys: make(map[string]*APIKey),
	}
}

// CreateAPIKey creates a new API key
func (akm *APIKeyManager) CreateAPIKey(userID, namespace string, roles []string, expiresAt *time.Time) (*APIKey, error) {
	key, err := GenerateAPIKey()
	if err != nil {
		return nil, err
	}

	apiKey := &APIKey{
		Key:       key,
		UserID:    userID,
		Namespace: namespace,
		Roles:     roles,
		CreatedAt: time.Now(),
		ExpiresAt: expiresAt,
		Active:    true,
	}

	akm.keys[key] = apiKey
	return apiKey, nil
}

// ValidateAPIKey validates an API key
func (akm *APIKeyManager) ValidateAPIKey(key string) (*APIKey, error) {
	apiKey, exists := akm.keys[key]
	if !exists {
		return nil, errors.New("invalid API key")
	}

	if !apiKey.Active {
		return nil, errors.New("API key is inactive")
	}

	if apiKey.ExpiresAt != nil && time.Now().After(*apiKey.ExpiresAt) {
		return nil, errors.New("API key expired")
	}

	return apiKey, nil
}

// RevokeAPIKey revokes an API key
func (akm *APIKeyManager) RevokeAPIKey(key string) error {
	apiKey, exists := akm.keys[key]
	if !exists {
		return errors.New("API key not found")
	}

	apiKey.Active = false
	return nil
}
