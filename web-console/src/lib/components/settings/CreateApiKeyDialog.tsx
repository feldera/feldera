import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { useHashPart } from '$lib/compositions/useHashPart'
import { showOnHashPart } from '$lib/functions/urlHash'
import { NewApiKeyRequest, NewApiKeyResponse } from '$lib/services/manager'
import { mutationGenerateApiKey } from '$lib/services/pipelineManagerQuery'
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import { FormContainer, TextFieldElement } from 'react-hook-form-mui'
import * as va from 'valibot'
import IconCopy from '~icons/bx/copy'
import IconX from '~icons/bx/x'

import { valibotResolver } from '@hookform/resolvers/valibot'
import { useClipboard } from '@mantine/hooks'
import {
  Box,
  Button,
  Dialog,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  List,
  ListItem,
  ListItemText,
  Stack,
  Typography,
  useTheme
} from '@mui/material'
import { useMutation, useQueryClient } from '@tanstack/react-query'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify API key name')]))
})

export type ApiKeyGeneratorSchema = va.Input<typeof schema>

export const CreateApiKeyDialog = () => {
  const { pushMessage } = useStatusNotification()
  const [generatedKeys, setGeneratedKeys] = useState([] as NewApiKeyResponse[])
  const queryClient = useQueryClient()
  const defaultValues: ApiKeyGeneratorSchema = {
    name: ''
  }
  const { mutate: generateApiKey } = useMutation(mutationGenerateApiKey(queryClient))
  const onGenerateApiKey = (data: NewApiKeyRequest) =>
    generateApiKey(data, {
      onSuccess(data) {
        setGeneratedKeys(old => old.concat(data))
      },
      onError(e) {
        pushMessage({ message: e.message, key: new Date().getTime(), color: 'error' })
      }
    })
  const { copy } = useClipboard()
  const hashPart = useHashPart()
  const router = useRouter()
  const { show } = showOnHashPart(hashPart)('new_api_key')
  const theme = useTheme()
  const handleClose = () => {
    setGeneratedKeys([])
    router.back()
  }
  return (
    <Dialog open={show} onClose={handleClose} fullWidth>
      <DialogTitle>Generate a new API key</DialogTitle>
      <DialogContent>
        <IconButton size='small' onClick={handleClose} sx={{ position: 'absolute', right: '1rem', top: '1rem' }}>
          <IconX />
        </IconButton>
        <FormContainer
          defaultValues={defaultValues}
          resolver={valibotResolver(schema)}
          onSuccess={onGenerateApiKey}
          FormProps={{ 'data-testid': 'box-form-generate-api-key' } as any}
        >
          <Stack spacing={4}>
            <DialogContentText color={theme.palette.warning.contrastText}>
              You will be shown the generated API key only once.
              <br />
              You will not be able to view it afterwards.
              <br />
              Please store it securely.
            </DialogContentText>
            <TextFieldElement name='name' size='small' label='Name'></TextFieldElement>
            <Box sx={{ display: 'flex', justifyContent: 'end' }}>
              <Button type='submit' variant='contained' color='primary'>
                Generate
              </Button>
            </Box>
          </Stack>
        </FormContainer>
        {generatedKeys.length ? <Typography variant='h6'>Generated API keys</Typography> : <></>}
        <List>
          {generatedKeys.map(key => (
            <ListItem
              key={key.api_key_id}
              secondaryAction={
                <IconButton onClick={() => copy(key.api_key)}>
                  <IconCopy fontSize={20} />
                </IconButton>
              }
            >
              <ListItemText
                primary={key.name}
                secondary={key.api_key}
                sx={{ wordWrap: 'break-word', pr: 2 }}
              ></ListItemText>
            </ListItem>
          ))}
        </List>
      </DialogContent>
    </Dialog>
  )
}
