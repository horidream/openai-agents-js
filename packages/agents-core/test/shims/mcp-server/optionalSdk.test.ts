import { afterEach, describe, expect, test, vi } from 'vitest';

// The MCP SDK is an optional dependency, so a missing install must only fail
// when a local MCP server connects, not when the package root is imported.
describe('Node MCP shim without the optional MCP SDK', () => {
  afterEach(() => {
    vi.doUnmock('@modelcontextprotocol/client');
    vi.resetModules();
  });

  test('imports the package root and fails on connect', async () => {
    vi.resetModules();
    vi.doMock('@modelcontextprotocol/client', () => {
      throw new Error('Cannot find package @modelcontextprotocol/client');
    });

    const { MCPServerStreamableHttp } = await import('../../../src/index');
    const server = new MCPServerStreamableHttp({
      url: 'https://example.test/mcp',
    });

    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      // Vitest wraps mock factory failures, keeping the import error as the cause.
      await expect(server.connect()).rejects.toHaveProperty(
        'cause.message',
        'Cannot find package @modelcontextprotocol/client',
      );
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('Failed to load the MCP SDK.'),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });
});
