type ServiceFilterOptions = {
	serviceId: string | null
	serviceIdColumn: string
	filters: string[]
	params: unknown[]
}

export const appendServiceFilter = ({
	serviceId,
	serviceIdColumn,
	filters,
	params
}: ServiceFilterOptions) => {
	if (!serviceId) {
		return
	}

	if (serviceId === 'weekday') {
		filters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = ${serviceIdColumn}
        AND (
          gc.monday = 1
          OR gc.tuesday = 1
          OR gc.wednesday = 1
          OR gc.thursday = 1
          OR gc.friday = 1
        )
    )`)
		return
	}

	if (serviceId === 'saturday') {
		filters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = ${serviceIdColumn}
        AND gc.saturday = 1
    )`)
		return
	}

	if (serviceId === 'sunday') {
		filters.push(`EXISTS (
      SELECT 1
      FROM gtfs_calendar gc
      WHERE gc.service_id = ${serviceIdColumn}
        AND gc.sunday = 1
    )`)
		return
	}

	params.push(serviceId)
	filters.push(`${serviceIdColumn} = $${params.length}`)
}
