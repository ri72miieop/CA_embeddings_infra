import { NodeSDK } from '@opentelemetry/sdk-node';
import { JaegerExporter } from '@opentelemetry/exporter-jaeger';
import { FastifyInstrumentation } from '@opentelemetry/instrumentation-fastify';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { SEMRESATTRS_SERVICE_NAME, SEMRESATTRS_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import { appConfig } from '../config/index.js';
import { logger } from './logger.js';

let sdk: NodeSDK | null = null;

export function initializeTracing(): void {
  if (!appConfig.observability.enableTracing) {
    logger.info('Tracing disabled');
    return;
  }

  try {
    const jaegerExporter = new JaegerExporter({
      endpoint: appConfig.observability.jaegerEndpoint || 'http://localhost:14268/api/traces',
    });

    sdk = new NodeSDK({
      resource: {
        attributes: {
          [SEMRESATTRS_SERVICE_NAME]: 'ca_embed',
          [SEMRESATTRS_SERVICE_VERSION]: '1.0.0',
        },
        merge: () => {},
      } as any,
      traceExporter: jaegerExporter,
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
    logger.info('Tracing initialized successfully');
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