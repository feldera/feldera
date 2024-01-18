'use client'

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { CreateApiKeyDialog } from '$lib/components/settings/CreateApiKeyDialog'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { mutationDeleteApiKey } from '$lib/services/pipelineManagerQuery'
import IconTrashAlt from '~icons/bx/trash-alt'
import Icon270RingWithBg from '~icons/svg-spinners/270-ring-with-bg'

import { Box, Button, IconButton, List, ListItem, ListItemText, Stack, Typography } from '@mui/material'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

export const ApiKeyList = () => {
  const { pushMessage } = useStatusNotification()
  const pipelineManagerQuery = usePipelineManagerQuery()
  const { data: apiKeys, isPending } = useQuery(pipelineManagerQuery.listApiKeys())
  const queryClient = useQueryClient()
  const { showDeleteDialog } = useDeleteDialog()
  const deleteKey = (
    ({ mutate }) =>
    (name: string) =>
      mutate(name, {
        onSuccess: () => {
          pushMessage({ message: 'Successfully deleted API key: ' + name, key: new Date().getTime(), color: 'success' })
        }
      })
  )(useMutation(mutationDeleteApiKey(queryClient)))
  const buttonGenerate = (
    <Box sx={{ display: 'flex', justifyContent: 'start' }}>
      <Button variant='contained' size='small' href='#new_api_key'>
        Generate new key
      </Button>
    </Box>
  )
  const title = <Typography variant='h6'>API keys</Typography>
  if (isPending) {
    return (
      <Stack spacing={4}>
        {title}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Icon270RingWithBg></Icon270RingWithBg>Fetching keys...
        </Box>
      </Stack>
    )
  }
  if (!apiKeys?.length) {
    return (
      <Stack spacing={4}>
        {title}
        No API keys generated
        {buttonGenerate}
        <CreateApiKeyDialog></CreateApiKeyDialog>
      </Stack>
    )
  }
  return (
    <Stack spacing={4}>
      {title}
      <List sx={{ maxHeight: '16rem', overflowY: 'auto' }}>
        {apiKeys.map(key => (
          <ListItem
            key={key.id}
            secondaryAction={
              <IconButton
                edge='end'
                onClick={() => showDeleteDialog('Revoke', name => `API key ${name}`, deleteKey)(key.name)}
              >
                <IconTrashAlt fontSize={20} />
              </IconButton>
            }
          >
            <ListItemText
              primary={key.name}
              secondary={'Permissions: ' + key.scopes.join(', ')}
              primaryTypographyProps={{
                sx: {
                  textOverflow: 'ellipsis',
                  overflowX: 'clip'
                }
              }}
            />
          </ListItem>
        ))}
      </List>
      {buttonGenerate}
      <CreateApiKeyDialog></CreateApiKeyDialog>
    </Stack>
  )
}
