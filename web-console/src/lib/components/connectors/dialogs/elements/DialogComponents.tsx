import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import Transition from '$lib/components/connectors/dialogs/tabs/Transition'
import { Dispatch, ReactNode, SetStateAction } from 'react'
import { FieldErrors, SubmitHandler } from 'react-hook-form'
import { FormContainer } from 'react-hook-form-mui'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { DialogTitle } from '@mui/material'
import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import IconButton from '@mui/material/IconButton'
import Tab from '@mui/material/Tab'

export function PlainDialogContent(props: { submitButton: ReactNode; children: ReactNode }) {
  return (
    <Box sx={{ height: '100%' }}>
      <Box sx={{ display: 'flex', flexDirection: 'column', p: 4, height: '100%' }}>
        {props.children}
        <Box sx={{ display: 'flex', justifyContent: 'end', pt: 4 }}>{props.submitButton}</Box>
      </Box>
    </Box>
  )
}

export function VerticalTabsDialogContent<Tabs extends string>(props: {
  tabs: readonly Tabs[]
  activeTab: Tabs
  setActiveTab: Dispatch<SetStateAction<Tabs>>
  tabList: {
    name: Tabs
    title: string
    description: string
    icon: JSX.Element
    testid: string
    content: JSX.Element
  }[]
}) {
  return (
    <TabContext value={props.activeTab}>
      <Box sx={{ display: 'flex', flexWrap: { xs: 'wrap', md: 'nowrap' }, height: '100%' }}>
        <Box>
          <TabList
            orientation='vertical'
            onChange={(e, newValue: Tabs) => props.setActiveTab(newValue)}
            sx={{
              border: 0,
              m: 0,
              '& .MuiTabs-indicator': { display: 'none' },
              '& .MuiTabs-flexContainer': {
                alignItems: 'flex-start',
                '& .MuiTab-root': {
                  width: '100%',
                  alignItems: 'flex-start'
                }
              }
            }}
          >
            {props.tabList.map(tab => (
              <Tab
                key={tab.name}
                disableRipple
                value={tab.name}
                label={
                  <TabLabel
                    title={tab.title}
                    subtitle={tab.description}
                    active={props.activeTab === tab.name}
                    icon={tab.icon}
                  />
                }
                data-testid={tab.testid}
              />
            ))}
          </TabList>
        </Box>
        <Box sx={{ width: '100%' }}>
          {props.tabList.map(tab => (
            <TabPanel
              key={tab.name}
              value={tab.name}
              sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
            >
              <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>{tab.content}</Box>
            </TabPanel>
          ))}
        </Box>
      </Box>
    </TabContext>
  )
}

export function ConnectorEditDialog<Schema extends va.BaseSchema<any, any>>(props: {
  show: boolean
  handleClose: () => void
  resolver: ReturnType<typeof valibotResolver<Schema>>
  values?: va.Input<Schema>
  defaultValues: va.Input<Schema>
  onSubmit: SubmitHandler<va.Input<Schema>>
  handleErrors: (errors: FieldErrors<Schema>) => void
  dialogTitle: string
  submitButton: JSX.Element
  disabled?: boolean
  children: ReactNode
}) {
  return (
    <Dialog
      fullWidth
      open={props.show}
      scroll='body'
      maxWidth='md'
      onClose={props.handleClose}
      TransitionComponent={Transition}
    >
      <FormContainer
        resolver={props.resolver}
        values={props.values}
        defaultValues={props.defaultValues}
        onSuccess={props.onSubmit}
        onError={props.handleErrors}
      >
        <DialogTitle sx={{ textAlign: 'center' }}>{props.dialogTitle}</DialogTitle>
        <IconButton
          onClick={props.handleClose}
          sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          data-testid='button-close-modal'
        >
          <i className={`bx bx-x`} />
        </IconButton>
        {props.children}
      </FormContainer>
    </Dialog>
  )
}
