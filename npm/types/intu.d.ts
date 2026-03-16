/**
 * intu-dev TypeScript SDK types.
 *
 * These types are automatically scaffolded into new projects at
 * src/types/intu.d.ts. They can also be imported directly from the
 * intu-dev package for plugin development:
 *
 *   import type { IntuMessage, IntuContext, IntuPlugin } from "intu-dev/types/intu";
 */

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
export type IntuMap = Record<string, JsonValue>;

export interface IntuHTTP {
  headers: Record<string, string>;
  queryParams: Record<string, string>;
  pathParams: Record<string, string>;
  method?: string;
  statusCode?: number;
}

export interface IntuFile {
  filename: string;
  directory: string;
}

export interface IntuFTP {
  filename: string;
  directory: string;
}

export interface IntuKafka {
  headers: Record<string, string>;
  topic: string;
  key: string;
  partition?: number;
  offset?: number;
}

export interface IntuTCP {
  remoteAddr: string;
}

export interface IntuSMTP {
  from: string;
  to: string[];
  subject: string;
  cc?: string[];
  bcc?: string[];
}

export interface IntuDICOM {
  callingAE: string;
  calledAE: string;
}

export interface IntuDatabase {
  query: string;
  params: Record<string, JsonValue>;
}

export interface IntuMessage {
  body: unknown;
  transport?: string;
  contentType?: string;
  sourceCharset?: string;
  metadata?: Record<string, unknown>;

  http?: IntuHTTP;
  file?: IntuFile;
  ftp?: IntuFTP;
  kafka?: IntuKafka;
  tcp?: IntuTCP;
  smtp?: IntuSMTP;
  dicom?: IntuDICOM;
  database?: IntuDatabase;
}

export interface IntuContext {
  channelId: string;
  correlationId: string;
  messageId: string;
  timestamp: string;
  stage?: string;
  inboundDataType?: string;
  outboundDataType?: string;
  destinationName?: string;
  sourceMessage?: IntuMessage;
  globalMap: IntuMap;
  channelMap: IntuMap;
  responseMap: IntuMap;
  connectorMap?: IntuMap;
}

export type IntuPluginPhase =
  | "before_validation"
  | "after_validation"
  | "before_transform"
  | "after_transform"
  | "before_destination"
  | "after_destination";

export type IntuPluginProcessFn = (msg: IntuMessage, ctx: IntuContext) => IntuMessage | void;

export interface IntuPlugin {
  name: string;
  phase: IntuPluginPhase;
  process: IntuPluginProcessFn;
}
