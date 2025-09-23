# Security Policy

## Supported Versions

We actively support the following versions of fluxdl with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| 0.x.x   | :x:                |

## Reporting a Vulnerability

The fluxdl team takes security vulnerabilities seriously. We appreciate your efforts to responsibly disclose your findings.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities to us privately:

- **Email**: [security@fluxdl.dev](mailto:security@fluxdl.dev)
- **Subject**: "fluxdl Security Vulnerability Report"

### What to Include

Please include the following information in your report:

- **Description** of the vulnerability
- **Steps to reproduce** the issue
- **Potential impact** and attack scenarios
- **Affected versions** of fluxdl
- **Your contact information** for follow-up questions

### Response Timeline

We will acknowledge receipt of your vulnerability report within **48 hours** and will send a more detailed response within **7 days** indicating the next steps in handling your report.

After the initial reply to your report, we will:
- Keep you informed of the progress towards a fix
- May ask for additional information or guidance
- Notify you when the vulnerability is fixed

## Security Best Practices

### For fluxdl Deployments

1. **Network Security**
   - Use TLS/SSL for all gRPC connections in production
   - Implement proper firewall rules
   - Restrict access to fluxdl ports (default: 9000)

2. **Authentication & Authorization**
   - Enable authentication for production deployments
   - Use strong passwords and rotate them regularly
   - Implement role-based access control (RBAC)

3. **Data Protection**
   - Encrypt sensitive data at rest
   - Use secure storage backends
   - Implement proper backup encryption

4. **Container Security**
   - Use official fluxdl Docker images
   - Keep Docker images updated
   - Run containers with non-root users
   - Scan images for vulnerabilities

5. **Monitoring & Logging**
   - Enable comprehensive logging
   - Monitor for suspicious activities
   - Set up alerting for security events

### For SDK Users

1. **Connection Security**
   - Always use secure connections in production
   - Validate server certificates
   - Implement connection timeouts

2. **Input Validation**
   - Validate all user inputs
   - Sanitize data before storage
   - Use parameterized queries where applicable

3. **Error Handling**
   - Don't expose sensitive information in error messages
   - Log security-relevant errors
   - Implement proper error recovery

## Known Security Considerations

### gRPC Security
- fluxdl uses gRPC for client-server communication
- Ensure TLS is enabled for production deployments
- Consider using mutual TLS (mTLS) for enhanced security

### Clustering Security
- Cluster communication should be secured
- Use private networks for cluster nodes
- Implement proper node authentication

### Data Storage
- BadgerDB storage is encrypted at rest by default
- Consider additional encryption for highly sensitive data
- Implement proper access controls for data directories

## Security Updates

Security updates will be:
- Released as soon as possible after verification
- Announced through GitHub Security Advisories
- Documented in release notes with severity levels
- Backported to supported versions when applicable

## Vulnerability Disclosure Policy

We follow responsible disclosure practices:

1. **Private Disclosure**: Report vulnerabilities privately first
2. **Investigation**: We investigate and develop fixes
3. **Coordination**: We coordinate with reporters on disclosure timing
4. **Public Disclosure**: We publish advisories after fixes are available

## Bug Bounty Program

We currently do not have a formal bug bounty program, but we:
- Acknowledge security researchers in our security advisories
- Provide public recognition for responsible disclosure
- Consider implementing a bounty program as the project grows

## Security Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [gRPC Security Guide](https://grpc.io/docs/guides/auth/)
- [Docker Security Best Practices](https://docs.docker.com/engine/security/)
- [Go Security Checklist](https://github.com/securego/gosec)

## Contact

For security-related questions or concerns:
- **Security Team**: [security@fluxdl.dev](mailto:security@fluxdl.dev)
- **General Questions**: [GitHub Discussions](https://github.com/skshohagmiah/fluxdl/discussions)

---

**Thank you for helping keep fluxdl and our community safe!** 🔒
