<script lang="ts">
	import { onMount } from 'svelte'
	import maplibregl from 'maplibre-gl'
	import { SEGMENT_HEAT_COLORS, SEGMENT_HEAT_THRESHOLDS } from '$lib/ui/segmentHeatmap'

	type SegmentProperties = {
		bunching_rate?: number | null
		has_data?: boolean
		from_stop_name?: string | null
		to_stop_name?: string | null
		total_headways?: number | null
	}

	type SegmentFeatureCollection = GeoJSON.FeatureCollection<GeoJSON.LineString, SegmentProperties>

	type Props = {
		segmentsGeoJson?: SegmentFeatureCollection | null
		selectedTimeBucket: string
	}

	let { segmentsGeoJson = null, selectedTimeBucket }: Props = $props()

	let mapContainer = $state<HTMLDivElement | null>(null)
	let map = $state<maplibregl.Map | null>(null)
	let mapLoaded = $state(false)
	let activePopup: maplibregl.Popup | null = null

	const styleUrl = import.meta.env.VITE_MAP_STYLE_URL ?? 'https://demotiles.maplibre.org/style.json'
	const streetGridSourceId = 'street-grid'
	const streetGridLayerId = 'street-grid-layer'
	const mediumThresholdPercent = Math.round(SEGMENT_HEAT_THRESHOLDS.medium * 100)
	const highThresholdPercent = Math.round(SEGMENT_HEAT_THRESHOLDS.high * 100)

	const emptyGeoJson: SegmentFeatureCollection = {
		type: 'FeatureCollection',
		features: []
	}

	const getSegmentsData = (): SegmentFeatureCollection => segmentsGeoJson ?? emptyGeoJson

	const getBounds = (geojson: SegmentFeatureCollection) => {
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
		if (!map || !mapLoaded) {
			return
		}
		const source = map.getSource('segments') as maplibregl.GeoJSONSource | undefined
		if (source) {
			source.setData(getSegmentsData())
		}
	}

	const fitToSegments = () => {
		if (!map || !segmentsGeoJson) {
			return
		}
		const bounds = getBounds(segmentsGeoJson)
		if (!bounds) {
			return
		}
		map.fitBounds(bounds, {
			padding: 40,
			maxZoom: 13,
			duration: 650
		})
	}

	const lightRasterPaint = {
		'raster-opacity': 1,
		'raster-saturation': -0.55,
		'raster-brightness-min': 0.15,
		'raster-brightness-max': 0.95,
		'raster-contrast': 0.14
	} as const

	const darkRasterPaint = {
		'raster-opacity': 0.85,
		'raster-saturation': -0.95,
		'raster-brightness-min': 0,
		'raster-brightness-max': 0.32,
		'raster-contrast': 0.45
	} as const

	const currentTheme = () =>
		typeof document !== 'undefined' &&
		document.documentElement.getAttribute('data-theme') === 'dark'
			? 'dark'
			: 'light'

	const applyRasterTheme = () => {
		if (!map || !map.getLayer(streetGridLayerId)) {
			return
		}
		const paint = currentTheme() === 'dark' ? darkRasterPaint : lightRasterPaint
		for (const [key, value] of Object.entries(paint)) {
			map.setPaintProperty(streetGridLayerId, key, value)
		}
	}

	const addStreetGridLayer = () => {
		if (!map) {
			return
		}

		if (!map.getSource(streetGridSourceId)) {
			map.addSource(streetGridSourceId, {
				type: 'raster',
				tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
				tileSize: 256,
				attribution: '© OpenStreetMap contributors'
			})
		}

		if (!map.getLayer(streetGridLayerId)) {
			map.addLayer({
				id: streetGridLayerId,
				type: 'raster',
				source: streetGridSourceId,
				paint: currentTheme() === 'dark' ? { ...darkRasterPaint } : { ...lightRasterPaint }
			})
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
			mapLoaded = true

			addStreetGridLayer()
			map?.addSource('segments', {
				type: 'geojson',
				data: getSegmentsData()
			})
			map?.addLayer({
				id: 'segments-base',
				type: 'line',
				source: 'segments',
				layout: {
					'line-cap': 'round',
					'line-join': 'round'
				},
				paint: {
					'line-width': 4.5,
					'line-color': SEGMENT_HEAT_COLORS.unknown,
					'line-opacity': 0.85
				}
			})
			map?.addLayer({
				id: 'segments-heat',
				type: 'line',
				source: 'segments',
				filter: ['==', ['get', 'has_data'], true],
				layout: {
					'line-cap': 'round',
					'line-join': 'round'
				},
				paint: {
					'line-width': [
						'interpolate',
						['linear'],
						['coalesce', ['get', 'bunching_rate'], 0],
						0,
						3.5,
						SEGMENT_HEAT_THRESHOLDS.medium,
						5,
						SEGMENT_HEAT_THRESHOLDS.high,
						7
					],
					'line-color': [
						'interpolate',
						['linear'],
						['coalesce', ['get', 'bunching_rate'], 0],
						0,
						SEGMENT_HEAT_COLORS.low,
						SEGMENT_HEAT_THRESHOLDS.medium,
						SEGMENT_HEAT_COLORS.medium,
						SEGMENT_HEAT_THRESHOLDS.high,
						SEGMENT_HEAT_COLORS.high
					],
					'line-opacity': 0.95
				}
			})
			fitToSegments()

			const formatRate = (rate: number | null | undefined) =>
				rate === null || rate === undefined ? 'No data' : `${(rate * 100).toFixed(1)}%`

			map?.on('click', 'segments-heat', (e) => {
				if (!e.features?.length || !map) return
				const props = e.features[0].properties as SegmentProperties
				const from = props.from_stop_name ?? '—'
				const to = props.to_stop_name ?? '—'
				const rate = formatRate(props.bunching_rate)
				const obs =
					props.total_headways !== null && props.total_headways !== undefined
						? props.total_headways.toLocaleString()
						: '—'

				activePopup?.remove()
				activePopup = new maplibregl.Popup({ closeButton: true, maxWidth: '260px' })
					.setLngLat(e.lngLat)
					.setHTML(
						`<div class="seg-popup">
							<div class="seg-popup-row"><span class="seg-label">From</span><span>${from}</span></div>
							<div class="seg-popup-row"><span class="seg-label">To</span><span>${to}</span></div>
							<div class="seg-popup-divider"></div>
							<div class="seg-popup-row"><span class="seg-label">Bunching</span><span class="seg-value">${rate}</span></div>
							<div class="seg-popup-row"><span class="seg-label">Observations</span><span>${obs}</span></div>
						</div>`
					)
					.addTo(map)
			})

			map?.on('mouseenter', 'segments-heat', () => {
				if (map) map.getCanvas().style.cursor = 'pointer'
			})
			map?.on('mouseleave', 'segments-heat', () => {
				if (map) map.getCanvas().style.cursor = ''
			})
		})

		const themeObserver = new MutationObserver(() => applyRasterTheme())
		themeObserver.observe(document.documentElement, {
			attributes: true,
			attributeFilter: ['data-theme']
		})

		return () => {
			themeObserver.disconnect()
			activePopup?.remove()
			map?.remove()
		}
	})

	$effect(() => {
		if (!map || !mapLoaded) {
			return
		}
		refreshSource()
		fitToSegments()
	})
</script>

<style>
	:global(.seg-popup) {
		font-family: 'IBM Plex Mono', ui-monospace, monospace;
		font-size: 12px;
		display: grid;
		gap: 4px;
	}
	:global(.seg-popup-row) {
		display: flex;
		justify-content: space-between;
		gap: 12px;
	}
	:global(.seg-label) {
		color: var(--text-muted);
		flex-shrink: 0;
	}
	:global(.seg-value) {
		font-weight: 700;
	}
	:global(.seg-popup-divider) {
		border-top: 1px solid var(--border);
		margin: 2px 0;
	}
	:global(:root[data-theme='dark'] .maplibregl-popup-content) {
		background: var(--surface-1);
		color: var(--text-strong);
	}
	:global(:root[data-theme='dark'] .maplibregl-popup-tip) {
		border-top-color: var(--surface-1);
		border-bottom-color: var(--surface-1);
	}
	:global(:root[data-theme='dark'] .maplibregl-popup-close-button) {
		color: var(--text-strong);
	}
	:global(:root[data-theme='dark'] .maplibregl-ctrl-group) {
		background: var(--surface-1);
	}
	:global(:root[data-theme='dark'] .maplibregl-ctrl-group button) {
		filter: invert(1) hue-rotate(180deg);
	}
</style>

<div class="panel visual-panel">
	<div class="section-head">
		<div>
			<p class="meta-line">Spatial view</p>
			<h3>Route segments</h3>
		</div>
		<small class="mono">{selectedTimeBucket.replace('_', ' ')}</small>
	</div>
	<div class="map-shell" bind:this={mapContainer}></div>
	<div class="map-legend" aria-label="Bunching heatmap legend">
		<div class="map-legend-item">
			<span class="legend-swatch low"></span>
			<span>Low (&lt;{mediumThresholdPercent}%)</span>
		</div>
		<div class="map-legend-item">
			<span class="legend-swatch medium"></span>
			<span>Medium ({mediumThresholdPercent}% - {highThresholdPercent}%)</span>
		</div>
		<div class="map-legend-item">
			<span class="legend-swatch high"></span>
			<span>High ({highThresholdPercent}%+)</span>
		</div>
		<div class="map-legend-item">
			<span class="legend-swatch unknown"></span>
			<span>No data</span>
		</div>
	</div>
</div>
