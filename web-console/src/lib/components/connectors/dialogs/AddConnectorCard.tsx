// Show cards for adding new connectors.
//
// Also attached the dialog for the connector that opens when someone clicks
// on Add.

import { SetStateAction, useState, Dispatch } from 'react'
import Button from '@mui/material/Button'
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import Typography from '@mui/material/Typography'
import { Icon } from '@iconify/react'

export interface AddConnectorCardProps {
  id?: string
  icon: string
  title: string
  dialog: React.ElementType<{ show: boolean; setShow: Dispatch<SetStateAction<boolean>> }>
}

export const AddConnectorCard = (props: AddConnectorCardProps) => {
  const [show, setShow] = useState<boolean>(false)

  return (
    <Card id={props.id}>
      <CardContent sx={{ textAlign: 'center', '& svg': { mb: 2 } }}>
        <Icon icon={props.icon} fontSize='4rem' />
        <Typography sx={{ mb: 3 }}>{props.title}</Typography>
        <Button variant='contained' onClick={() => setShow(true)}>
          Add
        </Button>
      </CardContent>
      <props.dialog show={show} setShow={setShow} />
    </Card>
  )
}
