/**
 * Browser-compatible MCP server implementations.
 * Uses StreamableHTTPClientTransport directly (bypassing Client) to avoid AJV/CSP issues.
 */
import {
  BaseMCPServerSSE,
  BaseMCPServerStdio,
  BaseMCPServerStreamableHttp,
} from '../../mcpShared';
import type {
  CallToolResult,
  CallToolResultContent,
  InitializeResult,
  MCPListResourcesParams,
  MCPListResourcesResult,
  MCPListResourceTemplatesResult,
  MCPReadResourceResult,
  MCPServerSSEOptions,
  MCPServerStdioOptions,
  MCPServerStreamableHttpOptions,
  MCPTool,
} from '../../mcp';
import { invalidateServerToolsCache } from '../../mcpToolCache';
import logger from '../../logger';

type MaybeSessionTransport = {
  close: () => Promise<void>;
  terminateSession?: () => Promise<void>;
  sessionId?: string;
};

const DEFAULT_REQUEST_TIMEOUT_MSEC = 60000;

function failedToImport(error: unknown): never {
  logger.error(
    `
Failed to load the MCP SDK. Please install the @modelcontextprotocol/sdk package.

npm install @modelcontextprotocol/sdk
    `.trim(),
  );
  throw error;
}

function hasSessionTransport(
  transport: any,
): transport is MaybeSessionTransport {
  return (
    transport != null &&
    typeof transport.close === 'function' &&
    (typeof transport.terminateSession === 'function' ||
      transport.sessionId !== undefined)
  );
}

/**
 * Creates a wrapped transport that skips the SSE GET request.
 * This is useful for servers that don't support SSE streaming (return 405).
 */
function createNoSseTransport(BaseTransport: any, url: URL, opts: any): any {
  const transport = new BaseTransport(url, opts);

  const originalStartOrAuthSse = transport._startOrAuthSse?.bind(transport);

  if (originalStartOrAuthSse) {
    transport._startOrAuthSse = async () => {
      return;
    };
  }

  return transport;
}

/**
 * MCPServerStdio is not supported in browser environments.
 * Stdio requires spawning child processes which is not available in browsers.
 */
export class MCPServerStdio extends BaseMCPServerStdio {
  constructor(params: MCPServerStdioOptions) {
    super(params);
  }

  get name(): string {
    return 'MCPServerStdio';
  }

  connect(): Promise<void> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }

  close(): Promise<void> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }

  listTools(): Promise<MCPTool[]> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }

  callTool(
    _toolName: string,
    _args: Record<string, unknown> | null,
    _meta?: Record<string, unknown> | null,
  ): Promise<CallToolResultContent> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }
  callToolResult(
    _toolName: string,
    _args: Record<string, unknown> | null,
    _meta?: Record<string, unknown> | null,
  ): Promise<CallToolResult> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }
  listResources(
    _params?: MCPListResourcesParams,
  ): Promise<MCPListResourcesResult> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }

  listResourceTemplates(
    _params?: MCPListResourcesParams,
  ): Promise<MCPListResourceTemplatesResult> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }

  readResource(_uri: string): Promise<MCPReadResourceResult> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }

  invalidateToolsCache(): Promise<void> {
    throw new Error(
      'MCPServerStdio is not supported in browser environments. Use MCPServerStreamableHttp or MCPServerSSE instead.',
    );
  }
}

/**
 * Lightweight MCP client that uses StreamableHTTPClientTransport directly.
 * Bypasses the MCP SDK's Client class to avoid AJV schema validation CSP issues.
 */
class LightweightMcpClient {
  private transport: any = null;
  private requestId = 0;
  private pendingRequests = new Map<
    number,
    { resolve: (value: any) => void; reject: (error: any) => void }
  >();

  constructor(transport: any) {
    this.transport = transport;
  }

  async connect(): Promise<InitializeResult> {
    // Set up message handler before starting
    this.transport.onmessage = (message: any) => {
      this.handleMessage(message);
    };

    this.transport.onerror = (error: any) => {
      logger.error('MCP transport error:', error);
    };

    // Start the transport
    await this.transport.start();

    // Send initialize request
    const result = await this.sendRequest('initialize', {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: {
        name: 'openai-agents-browser',
        version: '1.0.0',
      },
    });

    // Send initialized notification (no response expected)
    await this.sendNotification('notifications/initialized', {});

    return result as InitializeResult;
  }

  private handleMessage(message: any): void {
    // Handle JSON-RPC response
    if (message.id !== undefined && this.pendingRequests.has(message.id)) {
      const pending = this.pendingRequests.get(message.id)!;
      this.pendingRequests.delete(message.id);

      if (message.error) {
        pending.reject(
          new Error(message.error.message || JSON.stringify(message.error)),
        );
      } else {
        pending.resolve(message.result);
      }
    }
    // Notifications (no id) are ignored for now
  }

  private sendRequest(
    method: string,
    params?: Record<string, unknown>,
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      const id = ++this.requestId;
      this.pendingRequests.set(id, { resolve, reject });

      const message = {
        jsonrpc: '2.0' as const,
        id,
        method,
        params: params ?? {},
      };

      this.transport.send(message).catch((error: any) => {
        this.pendingRequests.delete(id);
        reject(error);
      });
    });
  }

  private async sendNotification(
    method: string,
    params?: Record<string, unknown>,
  ): Promise<void> {
    const message = {
      jsonrpc: '2.0' as const,
      method,
      params: params ?? {},
    };

    await this.transport.send(message);
  }

  async listTools(): Promise<any[]> {
    const result = await this.sendRequest('tools/list', {});
    return result?.tools ?? [];
  }

  async callToolResult(
    name: string,
    args: Record<string, unknown> | null,
    meta?: Record<string, unknown> | null,
  ): Promise<CallToolResult> {
    const params: Record<string, unknown> = {
      name,
      arguments: args ?? {},
    };
    if (meta != null) {
      params._meta = meta;
    }
    return (await this.sendRequest('tools/call', params)) as CallToolResult;
  }

  async listResources(params?: MCPListResourcesParams): Promise<any> {
    return this.sendRequest('resources/list', params);
  }

  async listResourceTemplates(params?: MCPListResourcesParams): Promise<any> {
    return this.sendRequest('resources/templates/list', params);
  }

  async readResource(uri: string): Promise<any> {
    return this.sendRequest('resources/read', { uri });
  }

  async close(): Promise<void> {
    if (this.transport) {
      await this.transport.close();
    }
    // Reject all pending requests
    for (const [_id, pending] of this.pendingRequests) {
      pending.reject(new Error('Client closed'));
    }
    this.pendingRequests.clear();
  }
}

/**
 * MCPServerStreamableHttp for browser environments.
 * Uses StreamableHTTPClientTransport directly with a lightweight client wrapper
 * to avoid AJV schema validation CSP issues while keeping streaming support.
 */
export class MCPServerStreamableHttp extends BaseMCPServerStreamableHttp {
  protected _cacheDirty = true;
  protected _toolsList: any[] = [];
  protected serverInitializeResult: InitializeResult | null = null;
  protected clientSessionTimeoutSeconds?: number;
  protected timeout: number;

  params: MCPServerStreamableHttpOptions;
  private _name: string;
  private client: LightweightMcpClient | null = null;
  private transport: any = null;

  constructor(params: MCPServerStreamableHttpOptions) {
    super(params);
    this.clientSessionTimeoutSeconds = params.clientSessionTimeoutSeconds ?? 5;
    this.params = params;
    this._name = params.name || `streamable-http: ${this.params.url}`;
    this.timeout = params.timeout ?? DEFAULT_REQUEST_TIMEOUT_MSEC;
  }

  async connect(): Promise<void> {
    try {
      const { StreamableHTTPClientTransport } =
        await import('@modelcontextprotocol/sdk/client/streamableHttp.js').catch(
          failedToImport,
        );

      const transportOpts = {
        authProvider: this.params.authProvider,
        requestInit: this.params.requestInit,
        fetch: this.params.fetch,
        reconnectionOptions: this.params.reconnectionOptions,
        sessionId: this.params.sessionId,
      };

      if (this.params.skipBrowserSseListener) {
        this.transport = createNoSseTransport(
          StreamableHTTPClientTransport,
          new URL(this.params.url),
          transportOpts,
        );
      } else {
        this.transport = new StreamableHTTPClientTransport(
          new URL(this.params.url),
          transportOpts,
        );
      }

      // Use lightweight client that bypasses AJV validation
      this.client = new LightweightMcpClient(this.transport);
      this.serverInitializeResult = await this.client.connect();
    } catch (e) {
      this.logger.error('Error initializing MCP server:', e);
      await this.close();
      throw e;
    }
    this.debugLog(() => `Connected to MCP server: ${this._name}`);
  }

  async invalidateToolsCache(): Promise<void> {
    await invalidateServerToolsCache(this.name);
    this._cacheDirty = true;
  }

  async listTools(): Promise<MCPTool[]> {
    if (!this.client) {
      throw new Error(
        'Server not initialized. Make sure you call connect() first.',
      );
    }
    if (this.cacheToolsList && !this._cacheDirty && this._toolsList) {
      return this._toolsList;
    }

    this._cacheDirty = false;
    const tools = await this.client.listTools();
    this.debugLog(() => `Listed tools: ${JSON.stringify(tools)}`);
    this._toolsList = tools;
    return this._toolsList;
  }

  async callTool(
    toolName: string,
    args: Record<string, unknown> | null,
    meta?: Record<string, unknown> | null,
  ): Promise<CallToolResultContent> {
    return (await this.callToolResult(toolName, args, meta)).content;
  }

  async callToolResult(
    toolName: string,
    args: Record<string, unknown> | null,
    meta?: Record<string, unknown> | null,
  ): Promise<CallToolResult> {
    if (!this.client) {
      throw new Error(
        'Server not initialized. Make sure you call connect() first.',
      );
    }
    const result = await this.client.callToolResult(toolName, args, meta);
    this.debugLog(
      () =>
        `Called tool ${toolName} (args: ${JSON.stringify(args)}, result: ${JSON.stringify(result)})`,
    );
    return result as CallToolResult;
  }

  async listResources(
    params?: MCPListResourcesParams,
  ): Promise<MCPListResourcesResult> {
    const { ListResourcesResultSchema } =
      await import('@modelcontextprotocol/sdk/types.js').catch(failedToImport);
    if (!this.client) {
      throw new Error(
        'Server not initialized. Make sure you call connect() first.',
      );
    }
    const response = await this.client.listResources(params);
    this.debugLog(() => `Listed resources: ${JSON.stringify(response)}`);
    return ListResourcesResultSchema.parse(response) as MCPListResourcesResult;
  }

  async listResourceTemplates(
    params?: MCPListResourcesParams,
  ): Promise<MCPListResourceTemplatesResult> {
    const { ListResourceTemplatesResultSchema } =
      await import('@modelcontextprotocol/sdk/types.js').catch(failedToImport);
    if (!this.client) {
      throw new Error(
        'Server not initialized. Make sure you call connect() first.',
      );
    }
    const response = await this.client.listResourceTemplates(params);
    this.debugLog(
      () => `Listed resource templates: ${JSON.stringify(response)}`,
    );
    return ListResourceTemplatesResultSchema.parse(
      response,
    ) as MCPListResourceTemplatesResult;
  }

  async readResource(uri: string): Promise<MCPReadResourceResult> {
    const { ReadResourceResultSchema } =
      await import('@modelcontextprotocol/sdk/types.js').catch(failedToImport);
    if (!this.client) {
      throw new Error(
        'Server not initialized. Make sure you call connect() first.',
      );
    }
    const response = await this.client.readResource(uri);
    this.debugLog(() => `Read resource ${uri}: ${JSON.stringify(response)}`);
    return ReadResourceResultSchema.parse(response) as MCPReadResourceResult;
  }

  get name() {
    return this._name;
  }

  get sessionId(): string | undefined {
    const transport = this.transport;
    return hasSessionTransport(transport) ? transport.sessionId : undefined;
  }

  async close(): Promise<void> {
    const transport = this.transport;

    if (hasSessionTransport(transport)) {
      const sessionId = transport.sessionId;

      if (sessionId && typeof transport.terminateSession === 'function') {
        try {
          await transport.terminateSession();
        } catch (error) {
          this.logger.warn('Failed to terminate MCP session:', error);
        }
      }
    }

    if (this.client) {
      await this.client.close();
      this.client = null;
    }
    this.transport = null;
  }
}

/**
 * MCPServerSSE is not yet implemented for browser environments.
 */
export class MCPServerSSE extends BaseMCPServerSSE {
  constructor(params: MCPServerSSEOptions) {
    super(params);
  }

  get name(): string {
    return 'MCPServerSSE';
  }

  connect(): Promise<void> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }

  close(): Promise<void> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }

  listTools(): Promise<MCPTool[]> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }

  callTool(
    _toolName: string,
    _args: Record<string, unknown> | null,
    _meta?: Record<string, unknown> | null,
  ): Promise<CallToolResultContent> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }
  callToolResult(
    _toolName: string,
    _args: Record<string, unknown> | null,
    _meta?: Record<string, unknown> | null,
  ): Promise<CallToolResult> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }
  listResources(
    _params?: MCPListResourcesParams,
  ): Promise<MCPListResourcesResult> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }

  listResourceTemplates(
    _params?: MCPListResourcesParams,
  ): Promise<MCPListResourceTemplatesResult> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }

  readResource(_uri: string): Promise<MCPReadResourceResult> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }

  invalidateToolsCache(): Promise<void> {
    throw new Error(
      'MCPServerSSE is not yet implemented for browser environments. Use MCPServerStreamableHttp instead.',
    );
  }
}
