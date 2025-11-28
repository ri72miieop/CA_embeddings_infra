import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { FastifyInstrumentation } from '@opentelemetry/instrumentation-fastify';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import { appConfig } from '../config/index.js';
import { logger } from './logger.js';

let sdk: NodeSDK | null = null;

export function initializeTracing(): void {
  if (!appConfig.observability.enableTracing) {
    logger.info('Tracing disabled');
    return;
  }

  try {
    const otlpExporter = new OTLPTraceExporter({
      url: appConfig.observability.otlpEndpoint || 'http://localhost:4318/v1/traces',
    });

    sdk = new NodeSDK({
      resource: {
        attributes: {
          [ATTR_SERVICE_NAME]: 'ca_embed',
          [ATTR_SERVICE_VERSION]: '1.0.0',
        },
        merge: () => {},
      } as any,
      traceExporter: otlpExporter,
      instrumentations: [
        new HttpInstrumentation({
          requestHook: (span, request) => {
            span.setAttributes({
              'http.request.header.user-agent': (request as any).headers?.['user-agent'] || '',
            });
          },
        }),
        new FastifyInstrumentation(),
      ],
    });

    sdk.start();
    logger.info('Tracing initialized successfully (OTLP -> Tempo)');
  } catch (error) {
    logger.error({ error }, 'Failed to initialize tracing');
  }
}

export function shutdownTracing(): Promise<void> {
  if (sdk) {
    return sdk.shutdown();
  }
  return Promise.resolve();
}

export { sdk };
