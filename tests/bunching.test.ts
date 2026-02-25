import { describe, expect, it } from 'vitest'
import { classifyBunching } from '../src/lib/server/headwayUtils'

describe('classifyBunching', () => {
	it('flags bunched and gapped based on scheduled headway', () => {
		const result = classifyBunching(2, 10)
		expect(result.bunched).toBe(true)
		expect(result.gapped).toBe(false)
	})

	it('flags super bunched at 1 minute or less', () => {
		const result = classifyBunching(1, 12)
		expect(result.super_bunched).toBe(true)
	})

	it('flags gapped when actual is 1.75x scheduled', () => {
		const result = classifyBunching(20, 10)
		expect(result.gapped).toBe(true)
	})
})
