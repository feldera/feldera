import { ServiceType } from '$lib/types/xgressServices'
import { AutocompleteElement } from 'react-hook-form-mui'
import { usePipelineManagerQuery } from 'src/lib/compositions/usePipelineManagerQuery'

import { useQuery } from '@tanstack/react-query'

export const PresetServiceElement = (props: {
  serviceType: ServiceType
  disabled?: boolean
  onChange?: (value: string | null) => void
  parentName: string
}) => {
  const PipelineManagerQuery = usePipelineManagerQuery()
  const { data } = useQuery(PipelineManagerQuery.listServices(null, null, props.serviceType))
  const services = data ?? []
  return (
    <>
      <AutocompleteElement
        name={props.parentName + '.preset_service'}
        label={
          services.length
            ? 'Optional: connect to a service'
            : 'No existing services, add a service or enter full connector details'
        }
        options={services.map(service => service.name)}
        autocompleteProps={{
          size: 'small',
          isOptionEqualToValue: (a, b) => a === b,
          onChange: (_e, value) => props.onChange?.(value)
        }}
      />
    </>
  )
}
