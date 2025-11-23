# Contributing to Distributed Job Scheduler

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing.

## 🚀 Getting Started

### Prerequisites

- Go 1.21 or later
- Docker and Docker Compose
- Git
- Make (optional but recommended)

### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/distributed-job-scheduler.git
   cd distributed-job-scheduler
   ```

2. **Install dependencies**
   ```bash
   go mod download
   ```

3. **Install development tools**
   ```bash
   make install-tools
   ```

4. **Run tests to verify setup**
   ```bash
   make test
   ```

## 🔧 Development Workflow

### 1. Create a Branch

Create a descriptive branch for your work:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

### 2. Make Changes

- Write clean, readable code
- Follow Go best practices and idioms
- Add tests for new functionality
- Update documentation as needed

### 3. Run Tests

Before committing, ensure all tests pass:

```bash
# Run unit tests
make test

# Run integration tests
make test-integration

# Run with coverage
make test-coverage

# Run linters
make lint
```

### 4. Format Code

```bash
make fmt
```

### 5. Commit Changes

Write clear, descriptive commit messages:

```bash
git commit -m "feat: add task priority reordering"
git commit -m "fix: resolve memory leak in worker pool"
git commit -m "docs: update API documentation"
```

Follow [Conventional Commits](https://www.conventionalcommits.org/):
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test additions/changes
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `chore:` Maintenance tasks

### 6. Push and Create Pull Request

```bash
git push origin your-branch-name
```

Then create a Pull Request on GitHub.

## 📝 Code Style

### Go Style Guidelines

- Follow the [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Use `gofmt` for formatting
- Use meaningful variable and function names
- Keep functions small and focused
- Write comments for exported functions and types
- Handle errors explicitly

### Example

```go
// ProcessTask processes a task and returns the result.
// It returns an error if the task cannot be processed.
func ProcessTask(ctx context.Context, task *models.Task) (*Result, error) {
    if task == nil {
        return nil, fmt.Errorf("task cannot be nil")
    }

    // Process the task
    result, err := executeTask(ctx, task)
    if err != nil {
        return nil, fmt.Errorf("failed to execute task: %w", err)
    }

    return result, nil
}
```

## 🧪 Testing Guidelines

### Unit Tests

- Write tests for all new code
- Use table-driven tests where appropriate
- Mock external dependencies
- Aim for >80% code coverage

```go
func TestTaskPriority(t *testing.T) {
    tests := []struct {
        name     string
        task     *models.Task
        expected int
    }{
        {"high priority", &models.Task{Priority: 10}, 10},
        {"low priority", &models.Task{Priority: 1}, 1},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            assert.Equal(t, tt.expected, tt.task.Priority)
        })
    }
}
```

### Integration Tests

- Use the `integration` build tag
- Test end-to-end workflows
- Clean up resources after tests

```go
//go:build integration
// +build integration

func TestSchedulerWorkflow(t *testing.T) {
    // Setup
    scheduler := setupTestScheduler(t)
    defer scheduler.Stop()

    // Test workflow
    // ...
}
```

## 📚 Documentation

### Code Documentation

- Document all exported functions, types, and constants
- Include examples for complex functionality
- Keep documentation up-to-date with code changes

### README and Guides

- Update README.md for user-facing changes
- Add examples for new features
- Update architecture diagrams if needed

## 🐛 Reporting Bugs

When reporting bugs, include:

1. **Description**: Clear description of the issue
2. **Steps to Reproduce**: Minimal steps to reproduce the bug
3. **Expected Behavior**: What you expected to happen
4. **Actual Behavior**: What actually happened
5. **Environment**: Go version, OS, etc.
6. **Logs**: Relevant log output

## 💡 Suggesting Features

For feature requests:

1. Check if the feature already exists
2. Describe the use case and benefits
3. Provide examples of how it would be used
4. Consider implementation complexity

## 🔍 Code Review Process

All submissions require review. We use GitHub pull requests for this purpose.

### Review Criteria

- Code quality and style
- Test coverage
- Documentation
- Performance impact
- Backward compatibility

### Getting Your PR Merged

1. Ensure all CI checks pass
2. Address reviewer feedback
3. Keep the PR focused and reasonably sized
4. Be responsive to comments

## 🏗️ Project Structure

```
distributed-job-scheduler/
├── cmd/                    # Main applications
│   ├── scheduler/         # Scheduler entry point
│   └── worker/            # Worker entry point
├── pkg/                   # Library code
│   ├── api/              # REST API
│   ├── consensus/        # Raft implementation
│   ├── scheduler/        # Core scheduling
│   ├── worker/           # Task execution
│   ├── storage/          # Persistence
│   ├── queue/            # Priority queue
│   ├── models/           # Data models
│   ├── grpc/             # gRPC services
│   ├── metrics/          # Prometheus metrics
│   └── logger/           # Logging
├── tests/                # Integration tests
├── examples/             # Usage examples
├── deployments/          # Deployment configs
└── config/               # Configuration files
```

## 🎯 Areas for Contribution

We welcome contributions in these areas:

### High Priority

- Performance optimizations
- Additional task executors
- Enhanced monitoring and metrics
- Security improvements
- Documentation improvements

### Medium Priority

- Additional storage backends
- Advanced scheduling algorithms
- Rate limiting enhancements
- Circuit breaker patterns

### Nice to Have

- Web UI improvements
- Additional examples
- Language SDKs (Python, JavaScript, etc.)
- Grafana dashboards

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License.

## 🤝 Community

- Be respectful and inclusive
- Help others learn and grow
- Share knowledge and best practices
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md)

## 📞 Getting Help

If you need help:

1. Check the documentation
2. Search existing issues
3. Ask in discussions
4. Open a new issue

Thank you for contributing! 🎉
