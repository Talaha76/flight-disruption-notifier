import { renderMessage } from '../../src/lib/notifications';
import { FlightEvent } from '../../src/schemas/flight-event';
import { delayEvent, gateEvent } from '../helpers/fixtures';

describe('renderMessage', () => {
  it('renders each disruption type', () => {
    expect(renderMessage(FlightEvent.parse(delayEvent())).text).toBe('Your flight AC123 on 2026-11-20 is delayed by 95 minutes. Reason: Weather.');
    expect(renderMessage(FlightEvent.parse(gateEvent())).text).toBe('The gate for your flight AC123 on 2026-11-20 has changed from C30 to C32.');
    const cx = renderMessage(FlightEvent.parse({ ...delayEvent(), type: 'CANCELLATION', reason: undefined }));
    expect(cx.subject).toBe('Flight AC123 cancelled');
    expect(cx.text).toContain('has been cancelled');
  });
});
