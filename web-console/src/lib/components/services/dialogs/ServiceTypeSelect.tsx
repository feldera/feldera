import { GridItems } from '$lib/components/common/GridItems'
import { SVGImport } from '$lib/types/imports'
import { ServiceType } from '$lib/types/xgressServices/ServiceDialog'
import KafkaLogo from '$public/images/vendors/kafka-logo-black.svg'
import { Dispatch, SetStateAction } from 'react'
import { match } from 'ts-pattern'

import { Button, Dialog, DialogContent, DialogTitle, Fade, Grid, useTheme } from '@mui/material'

const getServiceLogo = (serviceType: ServiceType): SVGImport =>
  match(serviceType)
    .with('kafka', () => KafkaLogo)
    .exhaustive()

const serviceTypes: ServiceType[] = ['kafka']

export const ServiceTypeSelectDialog = (props: { show: boolean; setShow: Dispatch<SetStateAction<boolean>> }) => {
  const theme = useTheme()
  return (
    <Dialog
      fullWidth
      open={props.show}
      scroll='body'
      maxWidth='md'
      onClose={() => props.setShow(false)}
      TransitionComponent={Fade}
      PaperProps={{ sx: { mb: '56vh' } }}
    >
      <DialogTitle sx={{ width: '100%', textAlign: 'center' }}>Select a type of service to register</DialogTitle>
      <DialogContent>
        <Grid container spacing={4}>
          <GridItems xs={4} sm={3} md={2}>
            {serviceTypes.map(serviceType => {
              const Logo = getServiceLogo(serviceType)
              return (
                <Button href={'#create/' + serviceType} key={serviceType}>
                  <Logo
                    style={{
                      objectFit: 'cover',
                      width: '100%',
                      aspectRatio: 1,
                      fill: theme.palette.text.primary
                    }}
                  ></Logo>
                </Button>
              )
            })}
          </GridItems>
        </Grid>
      </DialogContent>
    </Dialog>
  )
}
