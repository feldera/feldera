// ** MUI Imports
import Card from '@mui/material/Card'
import { styled } from '@mui/material/styles'
import CardHeader from '@mui/material/CardHeader'
import Typography from '@mui/material/Typography'
import CardContent from '@mui/material/CardContent'
import Grid, { GridProps } from '@mui/material/Grid'

const DemoGrid = styled(Grid)<GridProps>(({ theme }) => ({
  [theme.breakpoints.down('sm')]: {
    paddingTop: `${theme.spacing(1)} !important`
  }
}))

const TypographyTexts = () => {
  return (
    <Card>
      <CardHeader title='Texts' titleTypographyProps={{ variant: 'h6' }} />
      <CardContent>
        <Grid container spacing={6}>
          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>subtitle1</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography variant='subtitle1' sx={{ marginBottom: 2 }}>
              Cupcake ipsum dolor sit amet chocolate bar pastry sesame snaps.
            </Typography>
            <Typography variant='body2'>font-size: 16px / line-height: 28px / font-weight: 400</Typography>
          </DemoGrid>

          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>subtitle2</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography variant='subtitle2' sx={{ marginBottom: 2 }}>
              Cupcake ipsum dolor sit amet chocolate bar pastry sesame snaps.
            </Typography>
            <Typography variant='body2'>font-size: 14px / line-height: 21px / font-weight: 500</Typography>
          </DemoGrid>

          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>body1</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography sx={{ marginBottom: 2 }}>
              Cupcake ipsum dolor sit amet chocolate bar pastry sesame snaps.
            </Typography>
            <Typography variant='body2'>font-size: 16px / line-height: 24px / font-weight: 400</Typography>
          </DemoGrid>

          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>body2</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography variant='body2' sx={{ marginBottom: 2 }}>
              Cupcake ipsum dolor sit amet chocolate bar pastry sesame snaps.
            </Typography>
            <Typography variant='body2'>font-size: 14px / line-height: 21px / font-weight: 400</Typography>
          </DemoGrid>

          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>button</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography variant='button'>Button Text</Typography>
            <Typography variant='body2' sx={{ mt: 2 }}>
              font-size: 14px / line-height: 17px / font-weight: 500 / text-transform: uppercase
            </Typography>
          </DemoGrid>

          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>caption</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography variant='caption'>Cupcake ipsum dolor sit amet chocolate bar pastry sesame snaps.</Typography>
            <Typography variant='body2' sx={{ mt: 2 }}>
              font-size: 12px / line-height: 15px / font-weight: 400
            </Typography>
          </DemoGrid>

          <Grid item xs={12} sm={2} sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography>overline</Typography>
          </Grid>
          <DemoGrid item xs={12} sm={10}>
            <Typography variant='overline'>Cupcake ipsum dolor sit amet chocolate bar pastry sesame snaps.</Typography>
            <Typography variant='body2' sx={{ mt: 1 }}>
              font-size: 12px / line-height: 15px / font-weight: 400 / text-transform: uppercase
            </Typography>
          </DemoGrid>
        </Grid>
      </CardContent>
    </Card>
  )
}

export default TypographyTexts
