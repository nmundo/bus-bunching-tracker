import 'dotenv/config'

export const requireEnv = (key: string) => {
	const value = process.env[key]
	if (!value) {
		throw new Error(`${key} is not set`)
	}
	return value
}

export const optionalEnv = (key: string, fallback = '') => {
	return process.env[key] ?? fallback
}
