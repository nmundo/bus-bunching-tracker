<script lang="ts">
	import { onMount } from 'svelte'
	import maplibregl from 'maplibre-gl'

	type Props = {
		segmentsGeoJson?: GeoJSON.FeatureCollection<GeoJSON.LineString> | null
		selectedTimeBucket: string
	}

	let { segmentsGeoJson = null, selectedTimeBucket }: Props = $props()

	let mapContainer = $state<HTMLDivElement | null>(null)
	let map = $state<maplibregl.Map | null>(null)

	const styleUrl = import.meta.env.VITE_MAP_STYLE_URL ?? 'https://demotiles.maplibre.org/style.json'

	const getBounds = (geojson: GeoJSON.FeatureCollection<GeoJSON.LineString>) => {
		let minX = Infinity
		let minY = Infinity
		let maxX = -Infinity
		let maxY = -Infinity
		for (const feature of geojson.features) {
			for (const [lng, lat] of feature.geometry.coordinates) {
				minX = Math.min(minX, lng)
				minY = Math.min(minY, lat)
				maxX = Math.max(maxX, lng)
				maxY = Math.max(maxY, lat)
			}
		}
		if (minX === Infinity) {
			return null
		}
		return [
			[minX, minY],
			[maxX, maxY]
		] as maplibregl.LngLatBoundsLike
	}

	const refreshSource = () => {
		if (!map || !segmentsGeoJson) {
			return
		}
		const source = map.getSource('segments') as maplibregl.GeoJSONSource | undefined
		if (source) {
			source.setData(segmentsGeoJson)
		}
	}

	onMount(() => {
		if (!mapContainer) {
			return
		}
		map = new maplibregl.Map({
			container: mapContainer,
			style: styleUrl,
			center: [-87.6298, 41.8781],
			zoom: 10
		})
		map.addControl(new maplibregl.NavigationControl(), 'top-right')

		map.on('load', () => {
			map?.addSource('segments', {
				type: 'geojson',
				data: segmentsGeoJson ?? { type: 'FeatureCollection', features: [] }
			})
			map?.addLayer({
				id: 'segments-line',
				type: 'line',
				source: 'segments',
				paint: {
					'line-width': 4,
					'line-color': [
						'interpolate',
						['linear'],
						['get', 'bunching_rate'],
						0,
						'#2a9d8f',
						0.1,
						'#f2b453',
						0.2,
						'#d9412f'
					],
					'line-opacity': 0.85
				}
			})

			if (segmentsGeoJson) {
				const bounds = getBounds(segmentsGeoJson)
				if (bounds) {
					map?.fitBounds(bounds, { padding: 40, maxZoom: 13 })
				}
			}
		})

		return () => {
			map?.remove()
		}
	})

	$effect(() => {
		if (map && segmentsGeoJson) {
			refreshSource()
		}
	})
</script>

<div class="panel">
	<div style="display: flex; justify-content: space-between; align-items: baseline;">
		<h3>Route segments</h3>
		<small class="mono">{selectedTimeBucket}</small>
	</div>
	<div bind:this={mapContainer} style="height: 420px; border-radius: 16px; overflow: hidden;"></div>
</div>
