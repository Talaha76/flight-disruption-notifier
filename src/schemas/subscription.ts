import { z } from 'zod';
import { DATE_RE, FLIGHT_NUMBER_RE } from './flight-event';

export const Channel = z.enum(['email', 'sms', 'test']);

export const SubscriptionInput = z
  .object({
    userId: z.string().min(1).max(64).regex(/^[A-Za-z0-9_-]+$/, 'userId may only contain letters, digits, _ and -'),
    flightNumber: z.string().regex(FLIGHT_NUMBER_RE, 'flightNumber must look like AC123'),
    flightDate: z.string().regex(DATE_RE, 'flightDate must be YYYY-MM-DD'),
    channel: Channel,
    contact: z.string().min(1).max(254),
  })
  .superRefine((v, ctx) => {
    if (v.channel === 'email' && !z.email().safeParse(v.contact).success) {
      ctx.addIssue({ code: 'custom', path: ['contact'], message: 'contact must be a valid email for channel=email' });
    }
    if (v.channel === 'sms' && !/^\+[1-9]\d{7,14}$/.test(v.contact)) {
      ctx.addIssue({ code: 'custom', path: ['contact'], message: 'contact must be E.164 (e.g. +14165550123) for channel=sms' });
    }
  });

export type SubscriptionInput = z.infer<typeof SubscriptionInput>;
