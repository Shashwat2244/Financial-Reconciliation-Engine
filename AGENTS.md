# Codebase Rules for Agents

1. **Absolute Paths Only**: Always use absolute paths for file operations and configurations.
2. **No Temp Directories**: Never write project code files to `tmp` or `Desktop` folders. All work must remain within the designated project directory.
3. **Defensive Coding**: Prioritize modular, defensive code. Wrap external connections (like Snowflake or AWS) in `try/except` blocks with appropriate fallbacks.
4. **Environment Variables**: Always read credentials securely from `.env`. Never hardcode secrets.
