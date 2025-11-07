import type { FastifyRequest, FastifyReply } from 'fastify';
import { httpRequestsTotal, httpRequestDuration, activeConnections } from '../observability/metrics.js';

// Store start times for tracking request duration
const requestStartTimes = new Map<string, number>();

export const metricsMiddleware = async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
  const startTime = Date.now();
  requestStartTimes.set(request.id, startTime);

  activeConnections.inc();
};

// This will be called as an onResponse hook
export const metricsResponseHook = async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
  const startTime = requestStartTimes.get(request.id);
  if (startTime) {
    const duration = (Date.now() - startTime) / 1000;
    const route = (request as any).routeOptions?.url || request.url;
    const method = request.method;
    const statusCode = reply.statusCode.toString();

    httpRequestsTotal.inc({
      method,
      route,
      status_code: statusCode,
    });

    httpRequestDuration.observe(
      { method, route },
      duration
    );

    activeConnections.dec();
    requestStartTimes.delete(request.id);
  }
};