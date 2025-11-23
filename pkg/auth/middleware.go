package auth

import (
	"net/http"
	"strings"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// AuthMiddleware creates an authentication middleware
func AuthMiddleware(jwtManager *JWTManager, apiKeyManager *APIKeyManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Try JWT authentication first
		authHeader := c.GetHeader("Authorization")
		if authHeader != "" {
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) == 2 {
				switch parts[0] {
				case "Bearer":
					claims, err := jwtManager.ValidateToken(parts[1])
					if err != nil {
						c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token"})
						c.Abort()
						return
					}

					// Set user context
					c.Set("user_id", claims.UserID)
					c.Set("username", claims.Username)
					c.Set("roles", claims.Roles)
					c.Set("namespace", claims.Namespace)
					c.Next()
					return

				case "ApiKey":
					apiKey, err := apiKeyManager.ValidateAPIKey(parts[1])
					if err != nil {
						c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid API key"})
						c.Abort()
						return
					}

					// Set user context from API key
					c.Set("user_id", apiKey.UserID)
					c.Set("roles", apiKey.Roles)
					c.Set("namespace", apiKey.Namespace)
					c.Next()
					return
				}
			}
		}

		// No valid authentication
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		c.Abort()
	}
}

// RequirePermission creates a permission check middleware
func RequirePermission(roleManager *RoleManager, permission Permission) gin.HandlerFunc {
	return func(c *gin.Context) {
		roles, exists := c.Get("roles")
		if !exists {
			c.JSON(http.StatusForbidden, gin.H{"error": "No roles found"})
			c.Abort()
			return
		}

		roleNames, ok := roles.([]string)
		if !ok {
			c.JSON(http.StatusForbidden, gin.H{"error": "Invalid roles format"})
			c.Abort()
			return
		}

		if !roleManager.HasPermission(roleNames, permission) {
			logger.Warn("Permission denied",
				zap.Strings("roles", roleNames),
				zap.String("required_permission", string(permission)))

			c.JSON(http.StatusForbidden, gin.H{
				"error": "Insufficient permissions",
				"required": string(permission),
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// RequireNamespace ensures user can only access their namespace
func RequireNamespace() gin.HandlerFunc {
	return func(c *gin.Context) {
		userNamespace, exists := c.Get("namespace")
		if !exists {
			c.JSON(http.StatusForbidden, gin.H{"error": "No namespace found"})
			c.Abort()
			return
		}

		// Allow admin to access all namespaces
		roles, _ := c.Get("roles")
		if roleNames, ok := roles.([]string); ok {
			for _, role := range roleNames {
				if role == "admin" {
					c.Next()
					return
				}
			}
		}

		// Check if requested namespace matches user's namespace
		requestedNamespace := c.Query("namespace")
		if requestedNamespace == "" {
			requestedNamespace = c.Param("namespace")
		}

		if requestedNamespace != "" && requestedNamespace != userNamespace {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "Access denied to namespace",
				"namespace": requestedNamespace,
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// RateLimitMiddleware implements token bucket rate limiting
func RateLimitMiddleware(maxRequests int, window int64) gin.HandlerFunc {
	type bucket struct {
		tokens    int
		lastRefill int64
	}

	buckets := make(map[string]*bucket)

	return func(c *gin.Context) {
		userID, exists := c.Get("user_id")
		if !exists {
			userID = c.ClientIP()
		}

		key := userID.(string)
		now := time.Now().Unix()

		b, exists := buckets[key]
		if !exists {
			b = &bucket{
				tokens:    maxRequests,
				lastRefill: now,
			}
			buckets[key] = b
		}

		// Refill tokens
		if now-b.lastRefill >= window {
			b.tokens = maxRequests
			b.lastRefill = now
		}

		if b.tokens <= 0 {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "Rate limit exceeded",
				"retry_after": window - (now - b.lastRefill),
			})
			c.Abort()
			return
		}

		b.tokens--
		c.Next()
	}
}

// AuditMiddleware logs all API requests for audit trail
func AuditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID, _ := c.Get("user_id")
		username, _ := c.Get("username")
		namespace, _ := c.Get("namespace")

		logger.Info("API Request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Any("user_id", userID),
			zap.Any("username", username),
			zap.Any("namespace", namespace),
			zap.String("ip", c.ClientIP()),
		)

		c.Next()

		logger.Info("API Response",
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Any("user_id", userID),
		)
	}
}
