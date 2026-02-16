import { describe, expect, it } from 'vitest';
import { computeHeadways } from '../src/lib/server/headwayUtils';

describe('computeHeadways', () => {
  it('computes headways between consecutive arrivals', () => {
    const base = new Date('2024-01-01T12:00:00Z');
    const arrivals = [
      { vid: 'A', arrival_time: new Date(base.getTime() + 0 * 60000) },
      { vid: 'B', arrival_time: new Date(base.getTime() + 6 * 60000) },
      { vid: 'C', arrival_time: new Date(base.getTime() + 16 * 60000) }
    ];

    const headways = computeHeadways(arrivals);
    expect(headways).toHaveLength(2);
    expect(headways[0].headway_min).toBeCloseTo(6, 5);
    expect(headways[1].headway_min).toBeCloseTo(10, 5);
  });
});
