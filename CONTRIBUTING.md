# Contributing to fluxdl

Thank you for your interest in contributing to fluxdl! We welcome contributions from the community and are pleased to have you join us.

## 🌟 Ways to Contribute

- **🐛 Bug Reports** - Help us identify and fix issues
- **✨ Feature Requests** - Suggest new functionality
- **📝 Documentation** - Improve guides, examples, and API docs
- **🔧 Code Contributions** - Fix bugs, add features, optimize performance
- **🧪 Testing** - Write tests, test edge cases, performance testing
- **📚 SDK Development** - Improve existing SDKs or add new language support
- **🎨 Examples** - Create tutorials, demos, and use case examples

## 🚀 Quick Start for Contributors

### 1. Fork and Clone
```bash
# Fork the repository on GitHub, then:
git clone https://github.com/skshohagmiah/fluxdl.git
cd fluxdl
```

### 2. Build from Source
```bash
make build
./bin/fluxdl server
```

### 3. Set Up Development Environment
```bash
# Install Go 1.21+
go version

# Install dependencies
make deps

# Build the project
make build

# Run tests
make test
```

### 3. Create a Branch
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

## 📋 Development Guidelines

### Code Style

**Go Code:**
- Follow standard Go formatting (`gofmt`)
- Use meaningful variable and function names
- Add comments for public functions and complex logic
- Follow Go best practices and idioms

**Documentation:**
- Use clear, concise language
- Include code examples where helpful
- Update relevant documentation when changing APIs

### Commit Messages
Use conventional commit format:
```
type(scope): description

feat(kv): add TTL support for key-value operations
fix(queue): resolve memory leak in consumer groups
docs(sdk): update Go SDK installation guide
test(stream): add integration tests for partitioning
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

### Testing
- Write tests for new features
- Ensure existing tests pass
- Add integration tests for complex features
- Test with different configurations

```bash
# Run all tests
make test

# Run specific test
go test ./pkg/kv -v

# Run with race detection
go test -race ./...
```

## 🏗️ Project Structure

```
fluxdl/
├── cmd/                    # Binaries (server, CLI)
├── pkg/                    # Core packages
│   ├── kv/                # Key-Value store
│   ├── queue/             # Message queues  
│   ├── stream/            # Event streams
│   ├── server/            # gRPC server
│   └── cluster/           # Clustering
├── api/proto/             # gRPC definitions
├── sdks/                  # Multi-language SDKs
│   ├── go/               # Go SDK
│   ├── nodejs/           # Node.js SDK
│   └── python/           # Python SDK
├── storage/               # Storage backends
├── tests/                 # Integration tests
└── scripts/               # Build scripts
```

## 🔧 Development Tasks

### Adding a New Feature

1. **Design** - Discuss in GitHub Issues first
2. **API Design** - Update protobuf definitions if needed
3. **Implementation** - Write the core logic
4. **Testing** - Add comprehensive tests
5. **Documentation** - Update docs and examples
6. **SDK Updates** - Update relevant SDKs

### Working on SDKs

Each SDK should maintain API consistency:

**Go SDK:**
```go
client.KV.Set(ctx, "key", "value")
client.Queue.Push(ctx, "queue", "message")
client.Stream.Publish(ctx, "stream", "event")
```

**Node.js SDK:**
```javascript
await client.kv.set('key', 'value')
await client.queue.push('queue', 'message')
await client.stream.publish('stream', 'event')
```

**Python SDK:**
```python
await client.kv.set("key", "value")
await client.queue.push("queue", "message")
await client.stream.publish("stream", "event")
```

### Performance Considerations

- Benchmark performance-critical changes
- Consider memory usage and garbage collection
- Test with realistic data sizes
- Profile before and after optimizations

## 🐛 Bug Reports

When reporting bugs, please include:

- **fluxdl version** and platform
- **Steps to reproduce** the issue
- **Expected behavior** vs actual behavior
- **Error messages** and logs
- **Minimal code example** if applicable

Use our [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.md).

## ✨ Feature Requests

For new features:

- **Use case** - Why is this needed?
- **Proposed solution** - How should it work?
- **Alternatives** - Other approaches considered?
- **Breaking changes** - Any compatibility concerns?

Use our [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.md).

## 📚 Documentation Contributions

Documentation improvements are always welcome:

- **Installation guides** - Make setup easier
- **API documentation** - Clarify usage
- **Examples** - Real-world use cases
- **Tutorials** - Step-by-step guides

## 🧪 Testing Guidelines

### Unit Tests
```bash
# Test specific package
go test ./pkg/kv -v

# Test with coverage
go test -cover ./pkg/...
```

### Integration Tests
```bash
# Run integration tests (requires running server)
make test-integration
```

### Performance Tests
```bash
# Run benchmarks
go test -bench=. ./pkg/...
```

## 🔄 Pull Request Process

1. **Fork** the repository
2. **Create** a feature branch
3. **Make** your changes
4. **Test** thoroughly
5. **Update** documentation
6. **Submit** pull request

### PR Checklist
- [ ] Tests pass (`make test`)
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] Commit messages follow convention
- [ ] No breaking changes (or clearly documented)
- [ ] Performance impact considered

### Review Process
- Maintainers will review within 48 hours
- Address feedback promptly
- Squash commits before merge
- Celebrate your contribution! 🎉

## 🏷️ Release Process

fluxdl follows semantic versioning:
- **Major** (v2.0.0) - Breaking changes
- **Minor** (v1.1.0) - New features, backward compatible
- **Patch** (v1.0.1) - Bug fixes

## 🤝 Community

### Communication Channels
- **GitHub Issues** - Bug reports, feature requests
- **GitHub Discussions** - General questions, ideas
- **Discord** - Real-time chat (coming soon)

### Code of Conduct
Please read our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to providing a welcoming and inclusive environment for all contributors.

## 🎯 Good First Issues

Look for issues labeled `good first issue` or `help wanted`:
- Documentation improvements
- Adding examples
- Writing tests
- Small bug fixes
- SDK enhancements

## 🏆 Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Invited to maintainer discussions for significant contributions

## 📞 Getting Help

Need help contributing?
- Check existing [GitHub Issues](https://github.com/skshohagmiah/fluxdl/issues)
- Ask in [GitHub Discussions](https://github.com/skshohagmiah/fluxdl/discussions)
- Read the [Installation Guide](INSTALLATION.md)
- Review [SDK Documentation](sdks/README.md)

## 📄 License

By contributing to fluxdl, you agree that your contributions will be licensed under the [GNU Affero General Public License v3.0](LICENSE).

---

**Thank you for contributing to fluxdl!** 🚀

Every contribution, no matter how small, helps make fluxdl better for everyone.
