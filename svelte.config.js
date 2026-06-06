import adapter from '@sveltejs/adapter-vercel'
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte'

const config = {
	kit: {
		adapter: adapter({
			runtime: 'nodejs22.x'
		}),
		alias: {
			$components: 'src/lib/components',
			$server: 'src/lib/server'
		},
		experimental: {
			remoteFunctions: true
		}
	},
	preprocess: vitePreprocess({ script: true }),
	compilerOptions: {
		experimental: {
			async: true
		}
	}
}

export default config
