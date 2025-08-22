# Contributing to Graft

Thank you for your interest in contributing to Graft! We welcome contributions from the community and are grateful for your help in making this project better.

## Code of Conduct

By participating in this project, you are expected to uphold our Code of Conduct. Please report unacceptable behavior to the project maintainers.

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When creating a bug report, please include:

- **Clear description** of the problem
- **Steps to reproduce** the behavior
- **Expected behavior** vs actual behavior
- **Environment details** (Go version, OS, etc.)
- **Code samples** or configuration that demonstrates the issue

### Suggesting Enhancements

Enhancement suggestions are welcome! Please provide:

- **Clear description** of the enhancement
- **Use case** and motivation for the change
- **Proposed implementation** (if you have ideas)
- **Backward compatibility** considerations

### Pull Requests

1. **Fork** the repository
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/amazing-feature
   ```
3. **Make your changes** following our coding standards
4. **Add tests** for new functionality
5. **Run the test suite** to ensure everything passes
6. **Update documentation** if needed
7. **Commit your changes** with clear, descriptive messages
8. **Push to your fork** and create a pull request

## Development Setup

### Prerequisites

- Go 1.21 or later
- Git

### Getting Started

1. Clone your fork:
   ```bash
   git clone https://github.com/yourusername/graft.git
   cd graft
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Run tests to verify setup:
   ```bash
   go test ./...
   ```

4. Run examples to test functionality:
   ```bash
   cd examples/basic
   go run main.go
   ```

## Coding Standards

### Go Style

- Follow standard Go conventions and idioms
- Use `gofmt` and `golint` to ensure consistent formatting
- Write clear, self-documenting code with meaningful variable names
- Keep functions focused and reasonably sized

### Code Organization

- **Public API** should remain in `pkg/graft` package
- **Implementation details** belong in `internal/` packages
- **Tests** should be comprehensive and test both happy paths and error cases
- **Examples** should be simple, focused, and well-documented

### Documentation

- **Public functions** must have clear godoc comments
- **Complex logic** should include inline comments explaining the "why"
- **Examples** should include README files with setup instructions
- **API changes** must update relevant documentation

## Testing

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run tests for specific package
go test ./internal/adapters/queue

# Run with race detection
go test -race ./...
```

### Test Requirements

- **Unit tests** for all new functionality
- **Integration tests** for complex workflows
- **Examples** should be tested and documented
- **Test coverage** should be maintained or improved

### Writing Tests

- Use table-driven tests for multiple test cases
- Test both success and failure scenarios
- Use meaningful test names that describe what is being tested
- Clean up resources in tests (defer cleanup functions)

## Documentation

### What to Document

- **Public API** changes require godoc updates
- **New features** need examples and usage documentation
- **Breaking changes** must be clearly documented
- **Architecture changes** should update design documents

### Documentation Style

- Use clear, concise language
- Include code examples for public APIs
- Provide context for why changes were made
- Link to related issues or discussions

## Commit Messages

Use clear, descriptive commit messages:

```
type(scope): brief description

Longer explanation if needed, including:
- What changed and why
- Any breaking changes
- Related issue numbers

Fixes #123
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Maintenance tasks

## Release Process

Releases are managed by project maintainers. Contributors should:

- Ensure changes are thoroughly tested
- Update documentation for user-facing changes
- Follow semantic versioning principles
- Coordinate with maintainers for breaking changes

## Community

### Getting Help

- **GitHub Issues**: For bugs and feature requests
- **Discussions**: For questions and general discussion
- **Documentation**: Check docs/ directory for detailed guides

### Maintainer Response Times

We aim to respond to:
- **Security issues**: Within 24 hours
- **Bug reports**: Within 1 week
- **Feature requests**: Within 2 weeks
- **Pull requests**: Within 1 week

## Recognition

Contributors are recognized in:
- CHANGELOG.md for significant contributions
- GitHub contributors list
- Special thanks in release notes for major features

## License

By contributing to Graft, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to Graft! 🚀