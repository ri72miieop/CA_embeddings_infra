import { v4 as uuidv4 } from 'uuid';

export const generateCorrelationId = (): string => {
  return uuidv4();
};

export const extractCorrelationId = (headers: Record<string, string | string[] | undefined>): string => {
  const correlationId = headers['x-correlation-id'] || headers['correlation-id'];

  if (typeof correlationId === 'string') {
    return correlationId;
  }

  if (Array.isArray(correlationId) && correlationId.length > 0) {
    return correlationId[0]!;
  }

  return generateCorrelationId();
};